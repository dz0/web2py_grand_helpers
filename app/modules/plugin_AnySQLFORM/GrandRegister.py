# -*- coding: utf-8 -*-

# from gluon import current
from AnySQLFORM import *
from pydal.objects import SQLALL, Query

####### DALSELECT ##########
from plugin_joins_builder.joins_builder import build_joins_chain , get_referenced_table # uses another grand plugin
from gluon.http import HTTP # for grandregister render()

# from pydal/adapters/base.py
SELECT_ARGS = (
     'orderby', 'groupby', 'limitby', 'required', 'cache', 'left', 'distinct',
     'having', 'join', 'for_update', 'processor', 'cacheable',
     'orderby_on_limitby'
     )


def extend_with_unique(A, B):
    for b in B:
        if  str(b) not in map(str, A):
            A.append(b)

class DalView(Storage):
    """similar as DB set, but "packs" query into kwargs 
    and adds join_chains property (which can infer some usefull info for ReactiveSQLFORM)
    """



    def kwargs_4select(self):
        kwargs = {key:self[key] for key in SELECT_ARGS if self[key]}

        if self._translation:   # inject translated stuff
            if 'left' in kwargs and kwargs[ 'left' ]:
                kwargs[ 'left' ] = kwargs['left'][:] # clone
                extend_with_unique( kwargs['left'], self._translation[ 'left' ])
                # kwargs[ 'left' ] =  kwargs[ 'left' ] + self._translation[ 'left' ]
            else:
                kwargs[ 'left' ] =  self._translation[ 'left' ]

            kwargs['having'] = self._translation[ 'having' ]

        return kwargs

    def __init__(self, *fields, **kwargs):
        """
        important part is join_chains -- array of join_chain (see plugin_joins_builder) 
                         they can be reused by reactiveFORM... to figure out which tables' fields should be updated  
                         
        ps.:  "fields" mean more generally "columns" or "expressions". But for consistency I leave as "fields"...
        """
        self.fields = fields
        self.db = current.db


        for key in SELECT_ARGS+('query', 'left_given', 'join_given', 'left_join_chains', 'inner_join_chains', 'translator'):
            self[key] = kwargs.get(key)

        # self.translator = GrandTranslator( self.translate_fiels or [] , language_id=2 )

        if self.left and self.left_join_chains :
            raise RuntimeError("Overlapping args for left...join_chains, %s" % self.left_join_chains)

        if self.join and self.inner_join_chains :
            raise RuntimeError("Overlapping args for inner...join_chains, %s" % self.inner_join_chains)
        
        if not self.left :
            self.get_join('left') # default
            
        if not self.join :
            self.get_join('inner')

        self.kwargs = kwargs
            
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
                elif self.translator.is_translation(field):
                    main_table = expr.second.table
                    self.query = main_table
                    return self.query

    def translate(self):
        if self.translator:
            translated = self.translator.translate( [self.fields, self.query, self.having] )
            # tfields, tquery, thaving = translated.expr
            t = self._translation  = Storage()
            t.fields, t.query, t.having = translated.expr
            t.left = translated.left  # they should be given at the end of all left
            return t
        # else:
        #     self.translation_left = []



    def get_sql(self, try_translate=True):
        self.guarantee_table_in_query()
        if try_translate and self.translate():
            return self.db(self._translation.query)._select( *self._translation.fields, **self.kwargs_4select() )
        else:
            return self.db(self.query)._select(*self.fields, **self.kwargs_4select())
        
    def execute(self, try_translate=True): # usuall select
        self.guarantee_table_in_query()
        if try_translate and self.translate():
            print "DBG Translated sql 2:  ", self.db(self.query)._select(*self.fields, **self.kwargs_4select())
            return self.db(self._translation.query).select( *self._translation.fields, **self.kwargs_4select() )
        else:
            return self.db(self.query).select(*self.fields, **self.kwargs_4select())
        
    def get_grid_kwargs(self):
        return "TODO"
        
    # def __call__(self):
        # return self.execute()
        


#
# GrandDalView -- include translations
# class GrandRegister( Storage ):
class GrandRegister( object ):
    """
    search with register (and translations)
    should look sth like:
    >>> GrandRegister(
        columns = [ ],
        left_join_chains = [ [] ], # probably would be enough
        search_fields = [ ],
        search_fields_update_triggers = {None:[ ]},
        translate_fields = [],
    )
    
    mostly used for:
       form
       filter: query, having
       records_w2ui
    
    """
    def __init__( self,
                  columns,

                  left_join_chains = None, # probably would be enough
                  search_fields = None,
                  search_fields_update_triggers = None,
                  translate_fields = None,
                  response_view = "plugin_w2ui_grid/w2ui_grid.html",


                  **kwargs # form_factory
                ):

        request = current.request
        # for w2ui_grid response_view
        self.cid =  kwargs.pop('cid', request.function )
        self.grid_function =  kwargs.pop('grid_function', request.function)
        self.data_name =  kwargs.pop('data_name',  request.controller)


        self.columns  = columns
        self.left_join_chains = left_join_chains  # probably would be enough
        self.search_fields = search_fields
        # self.search_fields.append( SearchField('grid') )

        self.search_fields_update_triggers = search_fields_update_triggers


        self.translate_fields = translate_fields

        self.response_view = response_view

        self.kwargs = kwargs

        # kwargs.setdefault('form_factory', SQLFORM.factory) # TODO change to grand search_form..
        def my_grand_search_form(*fields, **kwargs):
            from applications.app.modules.searching import search_form as grand_search_form
            return grand_search_form(self.cid, *fields, **kwargs)

        kwargs.setdefault( 'form_factory', my_grand_search_form )
        # a bit smarter way -- in case   kwargs['form_factory'] is None
        # self.form_factory = kwargs.pop('form_factory', None) or  my_grand_search_form
        # kwargs['form_factory'] = self.form_factory

        self.search_form = QuerySQLFORM( *self.search_fields, **kwargs )
        self.search_fields = self.search_form.formfields  # UPDATES items to SearchField instances



        # self.left_join_chains = self.join_chains or [[]]
        # self.search_fiels = self.search_fiels or columns

        self.selection = DalView(*self.columns,  left_join_chains=self.left_join_chains, **kwargs )
        # self.colums = self.selection.fields

        # self.search_fields_update_triggers                    # TODO: for ReactiveForm
        # self.translate_fields                               # TODO: for GrandTranslator


    def w2ui_grid(self):
        # some workarounds for grand core stuff

        request = current.request
        response = current.response

        if self.response_view: response.view = self.response_view

        response.subtitle = "test  w2ui_grid"
        response.menu = []

        context = dict(
            cid = self.cid,
            w2grid_columns=[
                {'field': FormField(f).name, 'caption': f.label, 'size': "100%",
                 'sortable': isinstance(f, (Field, Expression)), 'resizable': True}
                    for f in self.columns
                ],
            grid_function=self.grid_function,  # or 'users_grid'
            data_name=self.data_name ,
            # w2grid_sort = [  {'field': w2ui_colname(db.auth_user.username), 'direction': "asc"} ]
            w2grid_sort=[{'field': FormField(self.columns[0]).name, 'direction': "asc"}],
            # table_name
            **self.kwargs
            # ,dbg = response.toolbar()
        )
        return context

    def form(self):
        # cid?
        context = self.w2ui_grid()
        context['form'] =  self.search_form
        return context

    def render(self):
        request = current.request
        response = current.response

        if request.vars._grid:

            rows = self.w2ui_grid_records()
            result = dict(status='success', records=rows)  # TODO: error, etc...

            # response.view = "generic.json"
            # return json(dict(status='success', records = rows ))
            # from gluon.serializers import json
            # raise HTTP( 200,  response.render("generic.json", result) )
            raise HTTP( 200,  response.json( result ) )

        else:
            raise HTTP(200, response.render(self.response_view, self.form() ) )


    # def search_filter(self):
    #     " query and having "
    #     return self.search_form().build_queries()


    # # SIMPLE DATA -- ok for w2ui_grid_records
    # def get_data_with_virtual(self):  # but what about column sequence?
    #     # filter out virtual fields
    #     virtual_fields = []
    #     db_expressions = []
    #     for f in self.columns:
    #         if isinstance(f, Field.Virtual):
    #             virtual_fields.append(f)
    #         else:
    #             db_expressions.append(f)
    #     # select
    #     data = self.selection.execute()
    #     # put back virtual fields
    #     for r in data:
    #         for vf in virtual_fields:
    #             r[vf.name] = vf.f(r)
    #
    #     return data


    def get_selection(self):
        """get selection by filter of current request """
        filter = self.search_form.build_queries()

        self.selection = DalView(*self.columns,
                                 query=filter.query, having=filter.having,

                                 left_join_chains=self.left_join_chains,
                                 # group order distinct
                                 **self.kwargs
                                 )

        return self.selection

    def w2ui_grid_records(self):

        # def get_row_field_value(record, colname, columns=None, sqlrows=None):
        #     """finds column value in rows by colname
        #     taken from SQLTABLE
        #     also tries to get some extra info (field)
        #     """
        #
        #     if not columns:
        #         columns = list(sqlrows.colnames)
        #     for colname in columns:
        #         matched_column_field = \
        #             db._adapter.REGEX_TABLE_DOT_FIELD.match(colname)
        #         if not matched_column_field:
        #             if "_extra" in record and colname in record._extra:
        #                 r = record._extra[colname]
        #                 row.append(TD(r))
        #                 continue
        #             else:
        #                 raise KeyError(
        #                     "Column %s not found (SQLTABLE)" % colname)
        #         (tablename, fieldname) = matched_column_field.groups()
        #         colname = tablename + '.' + fieldname
        #         try:
        #             field = sqlrows.db[tablename][fieldname]
        #         except (KeyError, AttributeError):
        #             field = None
        #         if tablename in record \
        #                 and isinstance(record, Row) \
        #                 and isinstance(record[tablename], Row):
        #             r = record[tablename][fieldname]
        #         elif fieldname in record:
        #             r = record[fieldname]
        #         else:
        #             raise SyntaxError('something wrong in Rows object')
        #         return r

        # in real usecase - we want to RENDER first
        def rows_rendered_flattened(rows):
            colnames = rows.colnames
            rows.compact = False
            rows = rows.render()  # apply represent methods

            # rows = [ r.as_dict() for r in rows ]  # rows.as_list()

            # flatten (with forsed .compact) --- some option in w2p might allow field instead of table.field if jus one table in play
            def flatten(rows_as_list):
                return[  { field if table == '_extra'   else table+'.'+field : val
                                            for table, fields in row.items()    for field, val in fields.items() }
                          for row in rows_as_list ]
            rows = flatten(rows)
            # rows = [colnames] + [[ row[col]  for col in colnames ] for row in rows ]
            # result =  TABLE(rows)  # nicer testing
            return rows

        # get rows
        rows = self.get_selection().execute()

        # map to w2ui colnames

        rows =  rows_rendered_flattened(rows)
        def map_w2ui_colnames(rows_flattened):
            rez = {}
            for col in self.columns:
                # key = str(col.target_expression)
                # src_key = str(col.target_expression if hasattr(col, 'target_expression')   else  col)
                src_key = str( FormField(col).target_expression )
                dest_key = FormField(col).name
                rez[dest_key] = rows_flattened[src_key]
            return rez

        rows =  [ map_w2ui_colnames( row) for row in rows ]

        def as_htmltable(rows, colnames):
            from gluon.html import TABLE
            return TABLE([colnames] + [[row[col] for col in colnames] for row in rows])

        # rows = as_htmltable(rows, [FormField(col).name for col in self.columns]) # for testing

        return rows



class GrandTranslator():
    def __init__(self, fields, language_id=None):
        db = self.db = current.db
        # self.db_adapter = self.db._adapter


        if not language_id and auth.is_logged_in():
                language_id = auth.user.language_id

        if not language_id:
            raise RuntimeError("No language defined for translating")

        self.language_id = language_id

        if fields:
            self.fields = self.db._adapter.expand_all(fields, [])
            self.try_auto_update_fields = False

        else:
            # find fields from DB

            self.try_auto_update_fields = True
            self.get_all_translatable_fields()

    def translation_alias(self, field):
        """aliased translations table
        """
        return self.db.translation_field.with_alias( "T_"+field._tablename+"__"+field.name )

    def translate_field(self, field):
        if str(field) in map(str, self.fields):  # direct check probably uses __eq__ for objects and returns nonsense
            t_alias = self.translation_alias( field )
            if not str(field) in  map(str, self.used_fields):
                self.used_fields.append( field )
            return  t_alias.value.coalesce( field )
            # return  self.adapter.COALESCE( t_alias.value , field)
        else:
            return field

    def generate_left_joins(self):
        joins = []
        for field in self.used_fields:
            t_alias = self.translation_alias(field)
            joins.append(
                t_alias.on(
                    (t_alias.tablename == field._tablename) &
                     (t_alias.fieldname == field.name) &  # for aliased fields might need different
                     (t_alias.rid == field._table._id) &
                     (t_alias.language_id == self.language_id)
                )
            )
        return joins

    def is_translation(self, expr):
        return (
              hasattr(expr, 'op') and expr.op is expr.db._adapter.COALESCE
              and isinstance(expr.second, Field)
              and str(expr.first) in [self.translation_alias(expr.second) + '.value', 'translation_field.value']
            )

    def translate(self, expression ):
        """Traverse Expression (or Query) tree and   decorate  fields with COALESCE translations
        returns:
           new expression
           left_joins  for translations
        """

        self.used_fields = [ ]
        # self.new_expression = Expression(db,lambda item:item)

        # maybe use ideas from https://gist.github.com/Xjs/114831
        def _traverse_translate( expr, inplace=False):
            # based on base adapter "expand"

            if expr is None:
                return None

            if isinstance(expr, Field):
                return  self.translate_field( expr )

            # prevent translations of in aggregates...
            #  self.db._adapter.COUNT is sensitive to translation
            # not sure about CONCAT   SUM of texts ?
            elif hasattr(expr, 'op') and expr.op is self.db._adapter.AGGREGATE :
                return expr

            #if we already have translation here
            elif self.is_translation(expr):
                return expr

            elif isinstance(expr, (Expression, Query)):
                first =  _traverse_translate(  expr.first )
                second =  _traverse_translate( expr.second )

                # if inplace:
                #     expr.first = first
                #     expr.second = second
                #     return

                return expr.__class__( expr.db, expr.op, first, second )
                # return Expression( expr.db, expr.op, first, second, expr.type )
                # return Query( expr.db, expr.op, first, second )

            elif isinstance(expr, SQLALL):
                  expr = expr._table.fields  # might be problems with flattening
                  return [_traverse_translate(e) for e in expr]

            elif isinstance(expr, (list, tuple )):
                flatten_ALL = []
                for e in expr:
                    if isinstance(expr, SQLALL): # expand and flatten
                        flatten_ALL.extend( expr._table.fields )
                    else:
                        flatten_ALL.append(e)

                return [_traverse_translate(e) for e in flatten_ALL]
            else:
                return expr

        new_expression = _traverse_translate( expression )
        self.result = Storage( expr=new_expression, left=self.generate_left_joins() )

        return self.result
