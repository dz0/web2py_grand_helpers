# -*- coding: utf-8 -*-

# from gluon import current
from AnySQLFORM import *
from gluon.html import URL, A, CAT, DIV, BEAUTIFY, PRE

from DalView import *
from GrandTranslator import *

def get_grid_kwargs(self):
        return "TODO"
        
    # def __call__(self):
        # return self.execute()
        
def create_fast_filters(field, values=None, search_field_name='__map2_SearchField'): #
    """this is kind of Widget
       search_field_name can be:  '__map2_SearchField' (default) or specified
       """

    # fast filters

    if search_field_name== '__map2_SearchField':
        sf = SearchField(field)
        search_field_name = sf.name
        field = sf.target_expression

    else:
        if isinstance(field, SearchField):
            sf = field  # field is already SearchField
            search_field_name = sf.name
            field = field.target_expression

        else:
            search_field_name = field.name


    fast_filters = [{'label': current.T('core__all'), 'selected': True, 'data': {}}, ]
    if values is None:
        # for row in field._db(field).select():
        #     fast_filters.append({'label': row[field.name], 'data': {sf.name: row[field.name]}})
        values = field._db(field).select().column(field) # or translated_set

    for val in values:
        if isinstance(val, dict):
            if 'data' in val and  'label' in val :
                filter = val
            else:
                raise RuntimeError("Wrong value for fast filter: %s" % val)

        if isinstance(val, (list, tuple)) and len(val)==2:
            filter = {'label': val[0], 'data': {search_field_name: val[1]}}
        else:  # single litteral
            filter = {'label': val, 'data': {search_field_name: val}}

        fast_filters.append(filter)

    return fast_filters

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
                  id_field = None,
                  left_join_chains = None, # probably would be enough
                  search_fields = None,
                  search_fields_update_triggers = None,
                  translate_fields = None,  # or translator??
                  response_view = "plugin_AnySQLFORM/w2ui_grid.html",

                  **kwargs # form_factory
                ):

        request = current.request
        # for w2ui_grid response_view
        self.w2ui_kwargs = Storage()
        self.w2ui_kwargs.cid           = self.cid           =  kwargs.pop('cid', request.function )
        self.w2ui_kwargs.grid_function = self.grid_function =  kwargs.pop('grid_function', request.function)

        self.w2ui_kwargs.table_name    = self.table_name    =  kwargs.pop('table_name',  columns[0]._tablename)
        self.w2ui_kwargs.data_name     = self.data_name     =  kwargs.pop('data_name',  self.table_name) or request.controller
        # self.w2ui_kwargs.context_name  = self.context_name  =  kwargs.pop('context_name', self.data_name) # maybe unnecessary
        self.w2ui_kwargs.crud_urls  = self.crud_urls  =  Storage( kwargs.pop('crud_urls', {} ) )
        crud_controller =  kwargs.pop('crud_controller', 'external' )

        if crud_controller in ['postback', None]:

            self.crud_urls.setdefault('add', URL(args=['add']))
            self.crud_urls.setdefault('edit', URL(args=['edit', '___id___'], vars={'view_extension':'html'}))  # todo: use signature?

        else:
            self.crud_urls.setdefault('add', URL(crud_controller, 'add_' + crud_controller))
            self.crud_urls.setdefault('edit', URL(crud_controller, 'edit_' + crud_controller, args=['___id___']))

        if  '___id___' not in self.crud_urls.edit:
            self.crud_urls.edit += '/___id___'  # though we risk in case of vars in URL

        self.w2ui_kwargs.w2grid_options_extra  =  kwargs.pop('w2grid_options_extra', {} )


        # ordinary params

        self.columns  = columns
        self.id_field = id_field or columns[0].table._id # will be passed in w2ui grid to Edit/Delete
        self.left_join_chains = left_join_chains  # probably would be enough
        self.search_fields = search_fields
        # self.search_fields.append( SearchField('grid') )

        self.search_fields_update_triggers = search_fields_update_triggers

        self.translate_fields = translate_fields

        self.response_view = response_view

        self.fast_filters = kwargs.get('filters') # actually they are addressed to grand_search_form

        self.kwargs = kwargs

        self.use_grand_search_form = kwargs.get('use_grand_search_form', True)
        if self.use_grand_search_form:
            # kwargs.setdefault('form_factory', SQLFORM.factory) # TODO change to grand search_form..
            def my_grand_search_form(*fields, **kwargs):
                from searching import search_form as grand_search_form
                return grand_search_form(self.cid, *fields, **kwargs)

            kwargs.setdefault( 'form_factory', my_grand_search_form )

        # a bit smarter way -- in case   kwargs['form_factory'] is None
        # self.form_factory = kwargs.pop('form_factory', None) or  my_grand_search_form
        # kwargs['form_factory'] = self.form_factory

        # self.search_form = QuerySQLFORM( *self.search_fields, **kwargs )
        self.search_form = GrandSQLFORM( *self.search_fields, **kwargs )
        self.search_fields = self.search_form.formfields  # UPDATES items to SearchField instances



        # self.left_join_chains = self.join_chains or [[]]
        # self.search_fiels = self.search_fiels or columns

        #~ self.selection = DalView(*self.columns,  left_join_chains=self.left_join_chains, **kwargs )
        
        # self.colums = self.selection.fields

        # self.search_fields_update_triggers                    # TODO: for ReactiveForm
        # self.translate_fields                               # TODO: for GrandTranslator


    def w2ui_grid_init(self):
        # some workarounds for grand core stuff
        # TODO: maybe refactor to separate W2ui_grid class?..

        response = current.response

        if self.response_view:
            response.view = self.response_view

        # response.subtitle = "test  w2ui_grid"
        # response.menu = response.menu or []

        self.w2ui_columns = [
                             {'field': FormField(f).name, 'caption': f.label, 'size': "100%",
                              'sortable': isinstance(f, (Field, Expression)), 'resizable': True}
                             for f in self.columns
                             ]
        self.w2ui_colnames = [d['field'] for d in self.w2ui_columns]  # parallely alligned to columns
        self.colnames = [ str(col) for col in self.columns ]          # parallely alligned to columns

        if getattr(self, 'w2ui_sort', None) is None:
            self.w2ui_sort = [{'field': self.w2ui_colnames[0] }]
        self.w2ui_sort[0].setdefault('direction', "asc")

        context = dict(

            w2ui_columns=self.w2ui_columns,

            w2ui_sort=self.w2ui_sort ,  # w2ui_sort = [  {'field': w2ui_colname(db.auth_user.username), 'direction': "asc"} ]

            # moved to w2ui_kwargs:
            # cid = self.cid,
            # table_name =
            # grid_function=self.grid_function,  # or 'users_grid'
            # data_name=self.data_name ,
            **self.w2ui_kwargs   # **self.kwargs

            # ,dbg = response.toolbar()
        )

        context.update(self.kwargs)

        return context

    def form(self):
        # cid?
        context = self.w2ui_grid_init()
        context['form'] =  self.search_form

        # for dbg purposes
        if not self.use_grand_search_form:

            ajax_url = "javascript:ajax('%s', %s, 'grid_records'); " % (
                URL(vars=dict(_grid=True, _grid_dbg=True), extension=None),
                [f.name for f in self.search_fields]
            )

            ajax_link = A('ajax load records', _href=ajax_url, _id="ajax_loader_link")
            ajax_result_target = DIV( "...AJAX RECORDS target...", _id='grid_records')

            context['form'] = CAT(ajax_result_target, ajax_link,
                                  context['form'] ,
                                  CAT("fast_filters", self.fast_filters)
                                  )

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

        elif request.args(0) in ['add', 'edit']:  # default CRUD actions
            action = request.args(0)
            table = self.id_field.table
            if action=='add':
                form = SQLFORM(table)
                form.process()
                raise HTTP(200, form=form)  # for ajax request
            if action=='edit':
                view_extension = request.vars.view_extension
                record = table(request.args(1)) # or redirect(..)
                form = SQLFORM( table , record )
                form.process()

                raise HTTP(200, response.render('core/base_form.html', dict(form=form, row_buttons=None))) # for full request
            # if form.process().accepted:
            #     response.flash = 'form accepted'
            # elif form.errors:
            #     response.flash = 'form has errors'


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


    def select(self):
        """get selection by filter of current request """
        filter = self.search_form.build_queries()
        
        # self.selection = DalView(
        rows = grand_select( 
                        self.id_field, *self.columns,
                        query=filter.query, having=filter.having,

                         left_join_chains=self.left_join_chains,
                         # group order distinct
                         **self.kwargs # translator inside
                         )
        
        if current.DBG:
            def tidy_SQL(sql):
                for w in 'from left inner where'.upper().split():
                    sql = sql.replace(w, '\n'+w)
                sql = sql.replace('AND', '\n      AND')
                return PRE(sql)
                    
            current.session.sql_log = map(tidy_SQL, rows.sql_log)
        # current.session.sql_nontranslated_log = map(tidy_SQL, rows.sql_nontranslated_log)
        
        # rows = self.selection.execute()
   
        return rows

    def w2ui_grid_records(self):
        self.w2ui_grid_init()

        # in real usecase - we want to RENDER first
        def rows_rendered_flattened(rows):
            colnames = rows.colnames
            # _compact = rows.compact
            rows.compact = False
            rows = rows.render()  # apply represent methods

            # TODO: maybe better use rawrows ?

            # rows = [ r.as_dict() for r in rows ]  # rows.as_list()

            # flatten (with forsed .compact) --- some option in w2p might allow field instead of table.field if jus one table in play
            def flatten(rows_as_list):
                return    [
                             { field if table == '_extra'   else table+'.'+field : val
                                for table, fields in row.items()     for field, val in fields.items()  }
                          for row in rows_as_list ]
            flat_rows = flatten(rows)

            # flat_rows.sql_log = rows.sql_log
            # flat_rows.sql_nontranslated_log = rows.sql_nontranslated_log
            # rows.compact = _compact
            # rows = [colnames] + [[ row[col]  for col in colnames ] for row in rows ]
            # result =  TABLE(rows)  # nicer testing
            return flat_rows

        # get rows
        initial_rows = self.select()

        # map to w2ui colnames

        rows =  rows_rendered_flattened(initial_rows)

        # list_of_colnames_map = [ dict(name_in_w2ui=FormField(col).name, name_in_db= str(col) )
        #                           for col in self.columns ]
        # def exec_map_w2ui_colnames(row_flat):
        #     return  { d['name_in_w2ui'] : row_flat[ d['name_in_db'] ] for d in list_of_colnames_map }
        # rows =  [ exec_map_w2ui_colnames( row)   for row in rows ]

        map_colnames_2_w2ui = dict( zip(self.colnames, self.w2ui_colnames ) )

        records =  [ ]

        id_field_str = str(self.id_field) # add w2ui recID

        for row in rows:
            r = {  map_colnames_2_w2ui[colname]:  row[ colname ]       for colname in self.colnames }
            r['recid'] = row[id_field_str]
            records.append( r )


        def as_htmltable(rows, colnames):
            from gluon.html import TABLE
            return TABLE([colnames] + [[row[col] for col in colnames] for row in rows])

        # records = as_htmltable(records, self.w2ui_colnames) # for testing

        return records




class GrandSQLFORM(QuerySQLFORM):
    """adds translator and uses it to generate validators_with_T """

    def __init__(self, *fields, **kwargs):

        self.translator = kwargs.pop('translator', None)

        # TODO: delete comment :)
        # gt = GrandTranslator(
        #     fields=[db.auth_user.first_name, db.auth_group.role],
        #     # we want to get tranlations only for first_name and role
        #     language_id=2
        # )
        # inject grand translation feature into AnySQLFORM
        # def default_IS_IN_DB(*args, **kwargs): return T_IS_IN_DB(gt, *args, **kwargs)

        kwargs.setdefault('table_name', 'GrandSQLFORM')
        QuerySQLFORM.__init__(self, *fields, **kwargs)



    def set_default_validator(self, f):

        if not self.translator:
            QuerySQLFORM.set_default_validator(self, f)
            return

        db = current.db  # todo: for Reactive form should be prefiltered dbset
        target = f.target_expression  # for brevity

        if f.type.startswith('reference ') or f.type.startswith('list:reference '):
            if f.requires == DEFAULT or getattr(f, 'use_default_IS_IN_DB', None):
                foreign_table = f.type.split()[1]
                foreign_table = foreign_table.split(':')[-1] # get rid of possible "list:"
                # f.requires = self.default_IS_IN_DB(db, db[foreign_table], db[foreign_table]._format)
                f.requires = T_IS_IN_DB(self.translator, db, db[foreign_table], db[foreign_table]._format)


        elif f.type in ('string', 'text'):
            if isinstance(target, Field):
                if f.comparison == 'equal':
                    f.requires = T_IS_IN_DB(self.translator, db, target)

                if f.comparison == 'contains':
                    # f.requires = IS_IN_DB(db, target)
                    # http://web2py.com/books/default/chapter/29/07/forms-and-validators#Autocomplete-widget
                    # f.widget = SQLFORM.widgets.autocomplete(
                    f.widget = T_AutocompleteWidget( self.translator,
                        current.request,
                        target
                        # , keyword='_autocomplete_%(tablename)s_%(fieldname)s__'+f.name # in case there would be 2 same targets
                        # , id_field=db.category.id
                    )

            if type(target) is Expression and f.comparison == 'equal':
            # should work for Field and Expression targets
                target = f.target_expression
                # theset = db(target._table).select(target).column(target)
                theset = DalView(target, translator=self.translator).execute().column(target)
                f.requires = IS_IN_SET(theset)


        else:
            QuerySQLFORM.set_default_validator(self, f)


