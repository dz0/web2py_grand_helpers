from gluon.storage import Storage
from gluon import current
from pydal.objects import Field, Row, Expression, Table
from gluon.html import PRE

from plugin_grand_helpers import extend_with_unique, append_unique, get_fields_from_table_format, is_reference, repr_data_Expression_Query_as_str
from plugin_grand_helpers import tidy_SQL, sql_log_format, get_sql_log, save_DAL_log, sql_log_find_last_pos, set_TIMINGSSIZE
from plugin_grand_helpers import append_unique, is_aggregate

####### DALSELECT ##########
from plugin_joins_builder import build_joins_chain , get_referenced_table # uses another grand plugin


def rename_row_fields(names_map, row, remove_src=True):
    """
    renames and REGROUPS columns (non compact mode)
    >>> r = Row('auth_user':{'first_name':'Jurgis', 'email':"m@il.as"}, '_extra': { 'count over(*)': 10})
    >>> # names_map = {'auth_user.first_name': 'auth.username',  '_extra.count over(*)':'auth.count' }
    >>> #names_map = {db.auth_user.first_name: 'auth.username',  '_extra.count over(*)':'auth.count' }
    >>> newr = rename_row_fields( names_map, r )

    newr == Row('auth_user':{'first_name':'Jurgis', 'email':"m@il.as"}, '_extra': { 'count over(*)': 10}))

    TODO: could use Adapter .parse(...) ??
    """

    db = current.db
    session = current.session
    _cached = session.col_2_table_and_field = session.col_2_table_and_field or {} # for caching stuff (in memory?)

    def parse_name(name):
        """can get Expression or str  and returns (tablename, fieldname)
        >>> parse_name("auth_user.username")
        ("auth_user", "username")
        """
        if isinstance(name, (list, tuple)):
            if len(name)==2:  return name
            else: raise ValueError( "Wrong name to parse: %s" % name )

        if isinstance(name, Expression):
            if expr in _cached:     return _cached[expr]
            expr = name
            name = str(name)
        else:
            expr = None

        if name in _cached:        return _cached[name]

        if name.count('.') == 1:
            table, field = name.split('.', 1)
            if table in db.tables:  # what if alias?
                _cached[name] = table, field
            else:
                raise ( "Suspicious name to parse -- not table (maybe alias?): %s" % name )
        else:
            _cached[name] = '_extra', name

        if expr:   _cached[expr] = _cached[name]  # cache in expression level

        return _cached[name]

    # result = defaultdict(dict)
    destination_fields = names_map.values()

    for a, b in names_map.items():

        atable, afield = parse_name(a)
        btable, bfield = parse_name(b)
        # print 'dbg', btable , bfield , '=',  atable , afield
        if btable in row   and bfield in row[btable]:
        #if row.get(btable, {}).get(bfield) is not None:  # if field already exists
            if destination_fields.count(b) > 1:  # if two/more same names
                msg = "duplicate destination field in map "
            else:
                msg = "field already was in row"
            raise RuntimeError("Overriding of field (loss of info):  %s.%s \n %s" % (btable, bfield, msg) ) # already existing in row or duplicate in map


        if not btable in row:
            row[btable] = Row()
        # row.setdefault(btable, Row()) # or {}
        row[btable][bfield] = row[atable][afield]
        if remove_src:
            del row[atable][afield]
            if not row[atable]: del row[atable] # if empty row left

    return row


# from pydal/adapters/base.py
SELECT_ARGS = (
     'orderby', 'groupby', 'limitby', 'required', 'cache', 'left', 'distinct',
     'having', 'join', 'for_update', 'processor', 'cacheable',
     'orderby_on_limitby','outer_scoped')




class DalView(Storage):
    """similar as DB set, but "packs" query into kwargs 
    and adds join_chains property (which can infer some usefull info for ReactiveSQLFORM)
    """

    def smart_orderby_prepend_distinct(self, kwargs):
        """for Postgre, when selecting distinct nonkeys, they should include the orderby"""
        db = current.db
        import pydal

        if isinstance(db._adapter, pydal.adapters.postgres.PostgreSQLAdapter):
            if kwargs.distinct:
                # kwargs.orderby. extend( kwargs.distinct )
                if isinstance(kwargs.distinct , (list, tuple)):
                    # extend_with_unique( kwargs.orderby, kwargs.distinct)
                    prepend_orderby = reduce(lambda a,b: a|b, kwargs.distinct )
                else:
                    prepend_orderby = kwargs.distinct

                if kwargs.orderby:
                    kwargs.orderby = prepend_orderby | kwargs.orderby
                else:
                    kwargs.orderby = prepend_orderby

                # if kwargs.distinct==True:
                #     kwargs.distinct = []
                #     extend_with_unique( kwargs.distinct, self.columns )
                #     extend_with_unique( kwargs.orderby, kwargs.distinct)

    def get_tables_of_expressions(self, expressions):
        db = current.db
        tables = db._adapter.tables # traverses expr and extracts tables

        tablenames = []
        for expr in expressions:
            for tablename in tables(expr):
                if not tablename in tablenames:
                    tablenames.append(tablename)

        return [db[tname] for tname in tablenames]


    def filter_out_translated_columns(self, fields=None):
        if self.translator and self.translation:
            fields = fields or self.translation.columns or []
            translated_fields = []
            if self.translation:
                # translated_fields_str = map(str, self.translation.affected_fields)
                for col in fields:
                    # if str(col) in translated_fields_str:
                    if self.translator.is_translation(col):
                        translated_fields.append(col)
            return translated_fields


    def smart_groupby_instead_of_distinct(self, just_repeat_nonagregates=True):
        """distinct gets wrong numbers for Window functions (COUNT OVER(*))
        so we construct needed groupby:
        :param smart_groupby_4distinct can be True or combination of "tables" "translations"
        """

        columns = self.translation.columns if (self.translator and self.translation) else self.columns
        print "DBG DalView columns:", map(str, columns)

        if just_repeat_nonagregates:
            # TOTAL_ROWS is also recognized as aggregate
            fields = cols_nonagregates = [col for col in  columns if not is_aggregate(col)]


        else:
            #  collect tables, involved in expressions, except the ones whose fields are aggregated
            cols_agregates = [ col for col in self.columns if is_aggregate(col) ]

            tables_with_agregates = self.get_tables_of_expressions ( cols_agregates )
            tables_all = self.get_tables_of_expressions ( self.columns )
            tables_without_agregates = [t for t in tables_all     if  t not in tables_with_agregates]
                                        # filter( lambda t: t not in tables_with_agregates, tables_all )
            tables_ids = [t.id for t in tables_without_agregates ]


            translations = self.filter_out_translated_columns() or [] # COALESCEs

            # fields = tables_ids + translated_cols
            fields = []
            if self.smart_groupby_4distinct is True:
                self.smart_groupby_4distinct = "tables translations"

            if 'tables' in self.smart_groupby_4distinct:
                fields += tables_ids
            if 'translations' in self.smart_groupby_4distinct:
                fields += translations

        # print("dbg) tables, translated_columns: ", map(str, translated_columns))
        if len(fields) > 1:
            return reduce( lambda a, b: a|b, fields )
        else:
            return fields[0]


    def kwargs_4select(self, translation=None):
        """
        constructs dictionary to be used in "select(**kwargs)"
        Takes initial values from self attributes, but doesn't override the attributes
        """
        kwargs = Storage( {key:self[key] for key in SELECT_ARGS if self[key]} )

        if translation:   # inject translated stuff
            for key in SELECT_ARGS:
                if key == 'left': continue # applied/appended below
                if key in translation:
                    kwargs[key] = translation[key]


            if kwargs.get( 'left' ):
                kwargs[ 'left' ] = kwargs['left'][:] # clone, to prevent influencing of passed list
                extend_with_unique( kwargs['left'], translation[ 'left' ])  # todo: might need optimisation
                # kwargs[ 'left' ] =  kwargs[ 'left' ] + self._translation[ 'left' ]
            else:
                kwargs[ 'left' ] =  translation[ 'left' ]


        if self.smart_groupby_4distinct:
            # self.distinct = self.distinct or True # set default True
            kwargs['distinct'] = None # disable in real selection

            if isinstance(self.distinct , Expression) :
                new = self.distinct  # should be    field1 | field2 | ..
            else:
                new = self.smart_groupby_instead_of_distinct()  # this should happen after translation


            if kwargs['groupby']:
                kwargs['groupby'] |= new
            else:
                kwargs['groupby'] = new

        if kwargs['distinct']:  # Note: not the same as self.distinct
            self.smart_orderby_prepend_distinct(kwargs)

        if hasattr(current, 'dev_limitby'):
            kwargs['limitby'] = kwargs['limitby'] or current.dev_limitby  # from models/dev.py
            kwargs['orderby_on_limitby'] = False

        if getattr(current, "DBG", None):
            # print "DBG_DalView_kwargs_4select:", kwargs
            print "DBG_DalView_kwargs_4select groupby:",  kwargs['groupby']
            print "DBG_DalView_kwargs_4select:", repr_data_Expression_Query_as_str(kwargs)
            current.session.DBG_DalView_kwargs_4select__LAST = repr_data_Expression_Query_as_str(kwargs) # FIXME: doesn't save on crash..,ex, ProgrammingError: for SELECT DISTINCT, ORDER BY expressions must appear in select list

        return kwargs




    def __init__(self, *columns, **kwargs):
        """
        important part is join_chains -- array of join_chain (see plugin_joins_builder) 
                         they can be reused by reactiveFORM... to figure out which tables' fields should be updated  
                         
        ps.:  "columns" can be items of Field | Expression | str .
        """

        self.db = current.db
        self.translation = None

        self.columns = columns

        for nr, col in enumerate(columns):
            if isinstance(col, Table):
                columns[nr] = col.ALL

        self.columns = self.db._adapter.expand_all(  self.columns, [] ) # for db.table.ALL  / SQLALL


        for key in SELECT_ARGS+('query',
                                'smart_groupby_4distinct',
                                'left_join_chain', 'inner_join_chain',
                                'left_join_chains', 'inner_join_chains',
                                'left_append', 'join_append',  # would be appended after join_chains # TODO maybe deprecate
                                'append_join_chains', # doesn't check duplication over ordinary left/join
                                'translator'):
            self[key] = kwargs.pop(key, None)

        def some_caution():
            assert not( self.left_join_chain and self.left_join_chains )
            assert not( self.inner_join_chain and self.inner_join_chains )

            # later we manipulate variable name with plural ...join_chainS, but when giving args, singular is most often used
            if self.left_join_chain: self.left_join_chains = [ self.left_join_chain ]
            if self.inner_join_chain: self.inner_join_chains = [ self.inner_join_chain ]

            if not self.append_join_chains:
                # self.translator = GrandTranslator( self.translate_fiels or [] , language_id=2 )
                from plugin_grand_helpers import  represent_joins
                if self.left and self.left_join_chains :
                    raise RuntimeError("Overlapping args for left...join_chains: \n left: %r\n left_join_chains: %r" % (represent_joins( self.left ), self.left_join_chains))

                if self.join and self.inner_join_chains :
                    raise RuntimeError("Overlapping args for inner...join_chains\n join: %r\n inner_join_chains: %r" % (represent_joins(self.join), self.inner_join_chains))

        some_caution()


        self.get_join('left') # default

        self.get_join('inner')

        self.kwargs = kwargs # not used...

    def get_join_chains( type_ = 'left'):
        #parse chains and return tablenames
        return "TODO" 
        
    def get_join(self, type_='left'): # TODO: better make left as @property
        #its a pitty, that there is left and join, but not left and inner properties...
            
        if type_=='left':
            if not self.left :
                self.left = []
            if self.left_join_chains:
                for jchain in self.left_join_chains:
                    self.left.extend( build_joins_chain(  *jchain ) )
            if self.left_append:
                self.left.extend(self.left_append)
            return self.left
              
        if type_=='inner':
            if not self.join : 
                self.join = []
            if self.inner_join_chains:
                for jchain in self.inner_join_chains:
                    self.join.extend( build_joins_chain(  jchain ) )
            if self.join_append:
                self.join.extend(self.join_append)
            return self.join



    def guarantee_table_in_query(self):
        if self.query == True: # this means "Everything"
            for expr in self.columns:
                if isinstance(expr, Field):
                    main_table = expr.table
                    self.query = main_table # main_table.id > 0
                    return self.query
                elif self.translator and self.translator.is_translation(field):
                    main_table = expr.second.table
                    self.query = main_table
                    return self.query

    def translate_expressions(self):  # todo: maybe make singleton based on self.translation state
        if self.translator:
            # we  translate all needed stuff in one call, so the generated "left" would not have duplicates
            t = self.translation = self.translator.translate( [self.columns, self.query, self.having, self.orderby, self.groupby, self.distinct ] )
            t.columns, t.query, t.having, t.orderby, t.groupby, t.distinct = t.pop('expr')

            if t.affected_fields:
                return t # also includes left, and affected_fields



    def get_sql(self, translate=True, t=None):
        self.guarantee_table_in_query()
        t = t or self.translate_expressions()
        if translate and t:
            sql = self.db(t.query)._select( *t.columns, **self.kwargs_4select( translation=t ) )
        else:
            sql = self.db(self.query)._select(*self.columns, **self.kwargs_4select())

        return tidy_SQL(sql, wrap_PRE=False)

    def execute(self, translate='transparent' or True or False, log=False ): # usuall select
        db = current.db

        self.guarantee_table_in_query()
        t = self.translate_expressions()
        if translate and t:
            # print "DBG Translated sql 2:  ", self.db(t.query)._select(*t.columns, **self.kwargs_4select( translation=t ))
            if getattr(current, 'DBG', False):
                print "\nDBG Bare sql:\n",    self.get_sql(translate=False, t=t)
                print "\nDBG Translated sql:\n", self.get_sql(translate=True, t=t)

            if log:        saved_debug, db._debug = db._debug, True
            trows = self.db(t.query).select(*t.columns, **self.kwargs_4select( translation=t ))
            if log:        db._debug = saved_debug

            # trows.compact = compact
            if translate == 'transparent':  # map fieldnames back to original (leave no COALESC... in Rows)
                map_2original_names = {str(t):str(f)   for t, f in zip(t.columns, self.columns) if str(t)!=str(f) } # todo: maybe use trows.parse

                trows.compact = False
                for row in trows:
                    rename_row_fields( map_2original_names, row )

                trows.colnames = [ map_2original_names.get( col , col )  for col in trows.colnames ]
                trows.compact = True
            return trows
        else:
            if log:        saved_debug, db._debug = db._debug, True
            rows = self.db(self.query).select(*self.columns, **self.kwargs_4select())
            if log:        db._debug = saved_debug

            # rows.compact = compact
            return rows


################ Virtual Fields in SELECT ####################
## with extension to have attrs: required_expressions, required_joins (means left joins)

def represent_table_asVirtualField(tablename, **kwargs):
    """virtual field to represent table's record by format"""
    db = current.db

    target_table = tablename
    fmt = db[target_table]._format

    vfield = Field.Virtual('ref_' + target_table,
                           f=lambda row: fmt % row.get(target_table, row),
                           label=tablename,
                           table_name=tablename
                           )

    vfield.required_expressions = [db[target_table][f] for f in get_fields_from_table_format(fmt)]
    vfield.orderby = reduce(lambda a, b: a|b, vfield.required_expressions )
    vfield.table = db[tablename]

    attrs_to_be_overriden = set(['required_expressions', 'orderby']) & set(kwargs.keys())
    if attrs_to_be_overriden:
        raise RuntimeWarning( "override of important attributes: %s" % attrs_to_be_overriden  ) # todo: update conflicting keys instead
    vfield.__dict__.update( kwargs ) # could override
    return vfield


def represent_FK(fk_field, **kwargs):
    """virtual field to represent foreign key (by joined table's format)"""

    # from helpers import is_reference
    # if not is_reference( fk_field):
    #     raise RuntimeError("non FK field")

    fk_field.represent = None  # disable default represent -- just in case

    target_table = fk_field.type.split()[1]

    vfield = represent_table_asVirtualField(target_table, **kwargs)
    vfield.label = fk_field.label  # use label of referencing field
    vfield.tablename = fk_field.tablename  # mark, that we use the FK table

    vfield.required_expressions.insert(0, fk_field)

    db = current.db
    vfield.required_joins = [db[target_table].on(db[target_table]._id == fk_field)]  # build_joins_chain(db.B, db.A)

    return vfield


def represent_PK(pk_field, **kwargs):
    """virtual field to represent private key (by table's format)"""

    # if isinstance(pk_field, Table):
    #     pk_field = pk_field._id

    target_table = pk_field.tablename

    return represent_table_asVirtualField(target_table, **kwargs)



# from gluon.storage import Storage
def agg_list_singleton(vfield, context_rows):
    """context_rows is Rows object, which has the stuff to get
    # maybe could use rows.join()  https://github.com/web2py/pydal/blob/3837691a943cf491572de289f822dcbad62e2b16/pydal/objects.py#L2803   https://groups.google.com/forum/#!topic/web2py/_xQUWYXZG54

    """

    # db_is_posgre = False
    # if db_is_posgre:  # TODO
    #     if isinstance(groupby, (list, tuple)):
    #          groupby = reduce(lambda a, b: a|b, groupby)
    # vfield.aggregate_select_kwargs['groupby'] = groupby

    # agg_expr = "json_agg(%s)" % expr

    # construct query

    db = current.db

    cache_name = 'grouped_4_' + vfield.name
    if hasattr(context_rows, cache_name):  # if cached
        grouped = getattr(context_rows, cache_name) # get chache
    else:
        agg_vars = vfield.aggregate = Storage(vfield.aggregate) # convert to Storage
        # if hasattr(vfield, 'required_joins'):
        #     agg_vars.select_kwargs.setdefault('left', vfield.required_joins)
        agg_vars.setdefault('groupby', db[vfield.tablename]._id)
        groupby = agg_vars.groupby

        ids = context_rows.column(groupby)
        query = groupby.belongs( set(ids) )
        if agg_vars.query:
            query &= agg_vars.query

        # ordinary select (no translation) would be
        # rows_4grouping = db(  query ).select(groupby,  *agg_vars.required_expressions, **agg_vars.select__kwargs)

        # DalView with translator
        selection = DalView(groupby,  # groupby needed  here for   .group_by_value()  # otherwise query would be enough
                            *agg_vars.required_expressions,
                            translator=agg_vars.translator ,
                            query=query,
                            **agg_vars.select__kwargs)

        rows_4grouping = selection.execute()

        grouped = rows_4grouping.group_by_value(groupby)  # todo: maybe use Rows.join(..) https://groups.google.com/forum/#!topic/web2py-developers/xpCJaD-GAcU
        # from collections import defaultdict
        # grouped = defaultdict(list)
        # grouped.update( rows_4grouping.group_by_value(groupby)  )

        grouped = Storage(grouped)
        # log sql
        if getattr(current, 'DBG', None):  # TODO change to LOG_SQL ?
            grouped.sql = selection.get_sql() 
            grouped.sql_nontranslated = selection.get_sql(translate=False)
        
        setattr(context_rows, cache_name,  grouped) # set cache
        

    return grouped



def select_with_virtuals(*columns,  **kwargs):
    """columns can be instance of Expression, Field, Field.Virtual

    acts similary as SQLFORM.grid, but returns Rows object.
    in the result: records are remapped according to columns,
    but rawrows and colnames stay as they are in the select...

    nonshown can indicate which columns to select (for virtuals) but exclude from result
    ps.: in SQLFORM.grid this is done with readable.False

    Example:
        Table A: f1, f2, vf3_agg (aggregate:required_expressions: B.id)
        Table B: f1, vf2(required_expressions: A.f1), f3

    columns = [ B.f1, B.vf2,  B.f3*3 ]
    -->
    virtual: [ B.vf2 ]
    selectable: [ B.f1, B.f3*3, A.f1 ]
    nonshown: [ A.f1 ]

    """
    db = current.db

    log = getattr(current, 'DBG', None)
    if log:

        saved_debug, db._debug = db._debug, True
        # set_TIMINGSSIZE(1000)
        # save_DAL_log(flush=True) # might be:

    sql_log = get_sql_log()
    sql_log_start = len( sql_log )
    sql_log_marker_item = sql_log[-1]

    delete_nonshown = kwargs.pop('delete_nonshown', True)

    dbset = kwargs.pop('dbset', None)
    if dbset is None:
        dbset = current.db

    query = dbset().query
    if 'query' in kwargs:
         if query:
             query &= kwargs.pop('query')
         else:
             query = kwargs.pop('query')
    
    if not columns:
        columns = kwargs.pop('columns')


    selectable = []
    virtual = [];    joins = []
    nonshown = kwargs.pop('nonshown', [])[:]  # used by virtual, but not included in result


    ### init: assign what is where
    for col in columns:
        if isinstance(col, Field.Virtual):
            if col not in virtual:
                virtual.append( col )

                # if getattr(col, 'aggregate', None):
                #     continue  # skip aggregateble vfields for now: todo: maybe add groupby to nonshown

                # look for dependances
                for required_expr in getattr(col, 'required_expressions', []):
                    # if required_expr not in nonshown:
                        # nonshown.append( required_expr )
                    append_unique(nonshown, required_expr )

                required_joins = getattr(col, 'required_joins', [])
                if required_joins:
                    # todo: maybe extend_with_unique: pseudocode: if diff(_tables(required_joins) , joined_tables): joins.extend( set_diff)
                    joins.extend(  required_joins )
        else:
            selectable.append( col )

    # make sure nonshown items doesn't appear in columns
    nonshown = set(nonshown).difference(columns)


    # more stuff to nonshow:    #  if not f.readable
    if kwargs.get('nonreadable_as_nonshown', False):
        nonreadable =   set ( f for f in columns if not f.readable)
        nonshown .update( nonreadable )
        columns = [  f for f in columns if f.readable ] # overrides original columns!

    nonshown = list(nonshown)


    ################################################################
    ####  do SELECT  -- get data rows
    ################################################################
    if joins:  # now joins always are left   TODO: make INNER possible..
        if not kwargs.get('left'):
            kwargs['left'] = []
            kwargs['left'].extend(  joins )

    translator= kwargs.pop('translator', None)  # TODO -- USE default translator
    selection = DalView( *(selectable+nonshown), translator=translator, query=query,  **kwargs )
    rows = selection.execute()

    if getattr(current, 'DBG', None):
        rows.sql = selection.get_sql()
        rows.sql_nontranslated = selection.get_sql(translate=False)
        rows.sql_log = [ rows.sql ]
        rows.sql_nontranslated_log = [ rows.sql_nontranslated ]
    # rows = dbset().select(*(selectable+nonshown), **kwargs)  # standart select


    # remap to resulting fields
    tmp_compact, rows.compact = rows.compact, False
    first_row = True  # for sql logs from virtual aggregates...
    for row in rows:
        # add virtual fields
        # this expects virtualfields to have tablename  (or could use tablenames_4_virtualfields)
        # https://github.com/web2py/web2py/blob/master/gluon/sqlhtml.py#L2862

        for field in virtual:
            if isinstance(field, Field.Virtual):
                    if not field.tablename in row:  # if virtual field is "orphan"  or no field from db[tablename] has been selected
                        row[field.tablename] = Row()

                    if hasattr(field, 'aggregate'): # aggregate virtuals always do extra join now (though they might reuse exixting rows (if all of them are selecteed))
                        id_field = field.aggregate['groupby']
                        group_id = row[id_field]
                        rows_4_aggregate = agg_list_singleton(vfield=field, context_rows=rows)
                        if getattr(current, 'DBG', None) and first_row:
                            rows.sql_log.append( rows_4_aggregate.sql )
                            rows.sql_nontranslated_log.append( rows_4_aggregate.sql_nontranslated )
                        group = rows_4_aggregate[group_id] or []
                        field.f = lambda r: field.aggregate.f(r, group) # is not called directly -- might be deleted?
                        # print 'dbg group', group
                        try:
                            value =field.aggregate.f(row, group)
                        except Exception as e:
                            import traceback
                            value = traceback.format_exc()
                            value += "\n Group: %r. \n Row: %r " %(group, row)
                            value = PRE(value,   _class='code',  _style="display: inline-block; word-wrap: break-word; word-break: break-all;white-space: pre-wrap; border:1px solid silver;")

                        row[field.tablename][field.name] = value

                    else:
                        # execute virtual function
                        # value = row[field.tablename][field.name]
                        value = field.f(row)
                        row[field.tablename][field.name] = value # this replaces call with value
                        # except KeyError:
                        #     value = dbset.db[field.tablename] [row[field.tablename][field_id]]  [field.name]


        # remove nonshown fields/expressions
        if delete_nonshown:
            for field in nonshown:
                del row[field.tablename][field.name]
                # if len(row[field.tablename]) == 0:
                if not row[field.tablename]:
                    del row[field.tablename]
        
        first_row = False  # first row finished 
        
    rows.compact = tmp_compact

    # rows.colnames = rows.colnames[:]
    # for field in nonshown:
    #     rows.colnames.remove(str(field))
    rows.rawcolnames = rows.colnames
    rows.colnames = [str(col) for col in columns]

    sql_log_start = sql_log_find_last_pos( sql_log_marker_item  ) + 1
    current.session.last_sql_with_virtuals = sql_log_format (get_sql_log(sql_log_start))

    if log:
        db._debug = saved_debug;
        # save_DAL_log(mode='a', flush=False)
        save_DAL_log()

    return rows

    # TODO: maybe apply
    def tablenames_4_virtualfields():
        db = dbset
        left = kwargs.get('left', [])
        fields = columns

        # taken from SQLFORM.grid https://github.com/web2py/web2py/blob/master/gluon/sqlhtml.py#L2326
        tablenames = db._adapter.tables(dbset.query)
        if left is not None:
            if not isinstance(left, (list, tuple)):
                left = [left]
            for join in left:
                tablenames += db._adapter.tables(join)
        tables = [db[tablename] for tablename in tablenames]
        if fields:
            # add missing tablename to virtual fields
            for table in tables:
                for k, f in table.iteritems():
                    if isinstance(f, Field.Virtual):
                        f.tablename = table._tablename
            columns = [f for f in fields if f.tablename in tablenames]




# class FieldVirtual_WithDependancies(Field.Virtual):
#     def __init__(self, name, f=None, ftype='string', label=None, table_name=None,
#                  required_expressions=[],  required_joins=[]):
#         Field.Virtual.__init__(self, name, f, ftype, label, table_name)
#         self.required_expressions = required_expressions
#         self.required_joins = required_joins
#
#
# class FieldVirtual_Aggregate(Field.Virtual):
#     pass
#     # def __init__(self, name, f=None, ftype='string', label=None, table_name=None):
#     #     Field.Virtual.__init__(self, name, f, ftype, label, table_name)

def virtual_field(  name, f,
                        ftype='string', label=None, table_name=None,  # standart Field.Virtual(..) kw_args
                        required_expressions = None,  # cols in select
                        required_joins = None

                    ):
    """field for select_with_virtuals, with `required_expressions` functionality """
    fv = Field.Virtual(name, f, ftype=ftype, label=label, table_name=table_name)
    fv.required_expressions = required_expressions or []
    fv.required_joins = required_joins or []

    return fv

def virtual_aggregated_field(name,
                            groupby,  # expression used to group stuff (also will be column in select)
                        required_expressions,  # cols in select
                        f_agg,  # aggregation lambda
                        f_group_item=None,  # function applied to group item/row -- like f for ordinary Field.Virtual

                        ftype='string', label=None, table_name=None,  # standart Field.Virtual(..) kw_args
                        translator=None,
                            query = None,
                            **select__kwargs  # probably mostly needed will be 'left' join
                            ):

    """
    constructs Field.Virtual which can aggregate fields...
    needs to use    agg_list_singleton    inside   select_with_virtuals

    Example:
            total_field_vagg = virtual_aggregate( 'total_field_vagg',
                query=query,
                groupby=db.warehouse_batch.good_id,  # expression used to group stuff (also will be column in select)
                required_expressions=db.warehouse_batch.ALL,  # cols in select
                f_agg = lambda r, group: D('0.00000')+sum(group),  # aggregation lambda
                f_group_item = lambda d: convert(db, d.price * d.residual, precision=5, source_currency_id=d.currency_id, rate_date=d.rate_date),  # function applied to group item/row -- like f for ordinary Field.Virtual
                table_name = 'warehouse_batch'
                #, translator = None
                # , left #** select__kwargs
            )
    """
    fv = Field.Virtual(name, f=lambda r: None, ftype=ftype, label=label, table_name=table_name)

    # we will not use f directly... (though it will be assigned inside select_with_virtuals(..)
    fv.f = None # workaround, as leaving f=None in args, would make "name" to be used as "f"...

    if f_group_item:
        f_agg1 = f_agg # just in case - to prevent (possible?) recursion
        f_agg2 = lambda row, group: f_agg1(  row,  map(f_group_item, group)    )  # apply aggregate function to  processed/represented items
        # f_agg3 = lambda row, group: None if group is None else f_agg2(row, group)   # automatically fallback to None if empty group
        f_agg = f_agg2

    fv.required_expressions = [groupby]  # this should be available per group

    fv.aggregate = dict(groupby=groupby,
                        required_expressions=required_expressions,
                        f=f_agg,
                        translator=translator,
                        query=query,
                        select__kwargs=select__kwargs,
                        )


    # fv.aggregate = dict(groupby=db.A.id,
    #                   select__kwargs=dict( left=[db.B.on(db.B.A_id == db.A.id)] ),
    #                   required_expressions=[db.B.id, db.B.f1],
    #                   # f =   lambda row, group:  ', '.join( map(, map(lambda r: r[db.B.id], group )) )
    #                   f=lambda row, group: ', '.join(map( lambda group_row: "%(id)s %(f1)s" % group_row.get(db.B, group_row),   group) )
    #                   , translator=gt
    #                   )


    return fv