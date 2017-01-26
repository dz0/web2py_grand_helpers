# -*- coding: utf-8 -*-

# from gluon import current
from AnySQLFORM import *

####### DALSELECT ##########
from plugin_joins_builder.joins_builder import build_joins_chain , get_referenced_table # uses another grand plugin

# from pydal/adapters/base.py
SELECT_ARGS = (
     'orderby', 'groupby', 'limitby', 'required', 'cache', 'left', 'distinct',
     'having', 'join', 'for_update', 'processor', 'cacheable',
     'orderby_on_limitby'
     )
     
class DalView(Storage):
    """similar as DB set, but "packs" query into kwargs 
    and adds join_chains property (which can infer some usefull info for ReactiveSQLFORM)
    """
    
    def kwargs_4select(self):
        return {key:self[key] for key in SELECT_ARGS if self[key]}
        
    def __init__(self, *fields, **kwargs):
        """
        important part is join_chains -- array of join_chain (see plugin_joins_builder) 
                         they can be reused by reactiveFORM... to figure out which tables' fields should be updated  
                         
        ps.:  "fields" mean more generally "columns" or "expressions". But for consistency I leave as "fields"...
        """
        self.fields = fields
        self.db = current.db
        
        for key in SELECT_ARGS+('query', 'left_join_chains', 'inner_join_chains'):
            self[key] = kwargs.get(key)
                    
    
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
            return self.left 
              
        if type_=='inner':
            if not self.join : 
                self.join = []
                if self.inner_join_chains:
                    for jchain in self.inner_join_chains:
                        self.join.extend( build_joins_chain(  jchain ) )
            return self.join      


    def guarantee_table_in_query(self):
        if self.query == True:
            main_table = self.fields[0].table
            self.query = main_table.id > 0

    def get_sql(self):
        self.guarantee_table_in_query()
        return self.db(self.query)._select( *self.fields, **self.kwargs_4select() )
        
    def execute(self): # usuall select
        self.guarantee_table_in_query()
        return self.db(self.query).select( *self.fields, **self.kwargs_4select() )
        
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

    def form_register(self):
        # cid?
        context = self.w2ui_grid()
        context['form'] =  self.search_form
        return context

    # def search_filter(self):
    #     " query and having "
    #     return self.search_form().build_queries()


    # # SIMPLE DATA -- ok for w2ui_records
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

    def records_w2ui(self):

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

        rows = as_htmltable(rows, [FormField(col).name for col in self.columns]) # for testing

        return rows



class GrandTranslator():
    pass


