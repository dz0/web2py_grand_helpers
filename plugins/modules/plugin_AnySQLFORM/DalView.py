from gluon.storage import Storage
from gluon import current
from pydal.objects import Field, Row, Expression

from helpers import extend_with_unique, append_unique, get_fields_from_table_format, is_reference

####### DALSELECT ##########
from plugin_joins_builder.joins_builder import build_joins_chain , get_referenced_table # uses another grand plugin
from gluon.http import HTTP # for grandregister render()

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

    def smart_distinct(self, kwargs):
        """for Postgre, when selecting distinct nonkeys, they should include the order"""
        if kwargs.distinct:
            kwargs.setdefault( 'orderby', [] )  # make it list (if it is not yet)
            # kwargs.orderby. extend( kwargs.distinct )
            if isinstance(kwargs.distinct , (list, tuple)):
                extend_with_unique( kwargs.orderby, kwargs.distinct)
            if kwargs.distinct==True:
                kwargs.distinct = []
                extend_with_unique( kwargs.distinct, self.fields )
                extend_with_unique( kwargs.orderby, kwargs.distinct)


    def smart_groupby(self, kwargs):  # todo
        """for Postgre - when selecting aggregates, other fields must be grouped"""
        pass

    def kwargs_4select(self, translation=None):
        kwargs = Storage( {key:self[key] for key in SELECT_ARGS if self[key]} )

        if translation:   # inject translated stuff
            if kwargs.get( 'left' ):
                kwargs[ 'left' ] = kwargs['left'][:] # clone, to prevent influencing of passed list
                extend_with_unique( kwargs['left'], translation[ 'left' ])
                # kwargs[ 'left' ] =  kwargs[ 'left' ] + self._translation[ 'left' ]
            else:
                kwargs[ 'left' ] =  translation[ 'left' ]

            kwargs['having'] = translation[ 'having' ]
            kwargs['orderby'] = translation[ 'orderby' ]


        # self.smart_distinct(kwargs)
        # self.smart_groupby(kwargs)

        if hasattr(current, 'dev_limitby'):
            kwargs['limitby'] = kwargs['limitby'] or current.dev_limitby  # from models/dev.py
            kwargs['orderby_on_limitby'] = False

        return kwargs

    def __init__(self, *fields, **kwargs):
        """
        important part is join_chains -- array of join_chain (see plugin_joins_builder) 
                         they can be reused by reactiveFORM... to figure out which tables' fields should be updated  
                         
        ps.:  "fields" mean more generally "columns" or "expressions". But for consistency I leave as "fields"...
        """
        self.columns = self.fields = fields
        self.db = current.db


        for key in SELECT_ARGS+('query', 'left_given', 'join_given', 'left_join_chains', 'inner_join_chains', 'translator'):
            self[key] = kwargs.pop(key, None)

        # self.translator = GrandTranslator( self.translate_fiels or [] , language_id=2 )

        if self.left and self.left_join_chains :
            raise RuntimeError("Overlapping args for left...join_chains, %s" % self.left_join_chains)

        if self.join and self.inner_join_chains :
            raise RuntimeError("Overlapping args for inner...join_chains, %s" % self.inner_join_chains)
        
        if not self.left :
            self.get_join('left') # default
            
        if not self.join :
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
                if self.left_given:
                    self.left.extend(self.left_given)
            return self.left
              
        if type_=='inner':
            if not self.join : 
                self.join = []
                if self.inner_join_chains:
                    for jchain in self.inner_join_chains:
                        self.join.extend( build_joins_chain(  jchain ) )
                if self.join_given:
                    self.join.extend(self.join_given)
            return self.join


    def guarantee_table_in_query(self):
        if self.query == True: # this means "Everything"
            for expr in self.fields:
                if isinstance(expr, Field):
                    main_table = expr.table
                    self.query = main_table # main_table.id > 0
                    return self.query
                elif self.translator and self.translator.is_translation(field):
                    main_table = expr.second.table
                    self.query = main_table
                    return self.query

    def translate_expressions(self):
        if self.translator:
            # we  translate all needed stuff in one call, so the generated "left" would not have duplicates
            t = self.translator.translate( [self.fields, self.query, self.having, self.orderby] )
            t.fields, t.query, t.having, t.orderby = t.pop('expr')
            # del t.orderby
            if t.affected_fields:
                return t # also includes left, and affected_fields



    def get_sql(self, translate=True, t=None):
        from helpers import tidy_SQL
        self.guarantee_table_in_query()
        t = t or self.translate_expressions()
        if translate and t:
            sql = self.db(t.query)._select( *t.fields, **self.kwargs_4select( translation=t ) )
        else:
            sql = self.db(self.query)._select(*self.fields, **self.kwargs_4select())

        return tidy_SQL(sql, wrap_PRE=False)

    def execute(self, translate='transparent' or True or False ): # usuall select
        self.guarantee_table_in_query()
        t = self.translate_expressions()
        if translate and t:
            # print "DBG Translated sql 2:  ", self.db(t.query)._select(*t.fields, **self.kwargs_4select( translation=t ))
            if getattr(current, 'DBG', False):
                print "\nDBG Bare sql:\n",    self.get_sql(translate=False, t=t)
                print "\nDBG Translated sql:\n", self.get_sql(translate=True, t=t)
            trows = self.db(t.query).select(*t.fields, **self.kwargs_4select( translation=t ))
            # trows.compact = compact
            if translate == 'transparent':  # map fieldnames back to original (leave no COALESC... in Rows)
                map_2original_names = {str(t):str(f)   for t, f in zip(t.fields, self.fields) if str(t)!=str(f) } # todo: maybe use trows.parse

                trows.compact = False
                for row in trows:
                    rename_row_fields( map_2original_names, row )

                trows.colnames = [ map_2original_names.get( col , col )  for col in trows.colnames ]
                trows.compact = True
            return trows
        else:
            rows = self.db(self.query).select(*self.fields, **self.kwargs_4select())
            # rows.compact = compact
            return rows


################ Virtual Fields in SELECT ####################
## with extension to have attrs: required_expressions, required_joins

def represent_table_asVirtualField(tablename):
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

    return vfield


def represent_FK(fk_field):
    """virtual field to represent foreign key (by joined table's format)"""

    # from helpers import is_reference
    # if not is_reference( fk_field):
    #     raise RuntimeError("non FK field")

    fk_field.represent = None  # disable default represent -- just in case

    target_table = fk_field.type.split()[1]

    vfield = represent_table_asVirtualField(target_table)
    vfield.label = fk_field.label  # use label of referencing field
    vfield.tablename = fk_field.tablename  # mark, that we use the FK table

    vfield.required_expressions.insert(0, fk_field)

    db = current.db
    vfield.required_joins = [db[target_table].on(db[target_table]._id == fk_field)]  # build_joins_chain(db.B, db.A)

    return vfield


def represent_PK(pk_field):
    """virtual field to represent private key (by table's format)"""

    target_table = pk_field.tablename

    return represent_table_asVirtualField(target_table)



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

        # ordinary select
        # rows_4grouping = db(  query ).select(groupby,  *agg_vars.required_expressions,
        #                                                                 **agg_vars.select__kwargs)


        # DalView with translator
        translator = agg_vars.translator # or use global
        selection = DalView(groupby,  *agg_vars.required_expressions,
                            translator=translator, query=query, **agg_vars.select__kwargs)

        rows_4grouping = selection.execute()
        
        

        grouped = rows_4grouping.group_by_value(groupby)  # todo: maybe use Rows.join(..)
        
        # log sql
        if current.DBG:
            grouped = Storage( grouped )
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

                if getattr(col, 'aggregate', None):
                    continue  # skip aggregateble vfields

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
        kwargs.setdefault('left', []).extend(  joins )

    translator= kwargs.pop('translator', None)  # TODO -- USE default translator
    selection = DalView( *(selectable+nonshown), translator=translator, query=query,  **kwargs )
    rows = selection.execute()
    if current.DBG:
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
            if isinstance(field, Field.Virtual) and field.tablename in row:

                    if hasattr(field, 'aggregate'): # aggregate virtuals always do extra join now (though they might reuse exixting rows (if all of them are selecteed))
                        id_field = field.aggregate['groupby']
                        group_id = row[id_field]
                        rows_4_aggregate = agg_list_singleton(vfield=field, context_rows=rows)
                        if current.DBG and first_row: 
                            rows.sql_log.append( rows_4_aggregate.sql )
                            rows.sql_nontranslated_log.append( rows_4_aggregate.sql_nontranslated )
                        group = rows_4_aggregate[group_id]
                        field.f = lambda r: field.aggregate.f(r, group) # is not called directly
                        row[field.tablename][field.name] = field.aggregate.f(row, group)

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


def grand_select(*args, **kwargs):
    return select_with_virtuals(*args, **kwargs)
    # return DalView(*args, **kwargs).execute()
