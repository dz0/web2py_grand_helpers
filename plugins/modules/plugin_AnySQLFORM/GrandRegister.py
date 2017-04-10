# -*- coding: utf-8 -*-

# from gluon import current
from AnySQLFORM import *
from gluon.html import URL, A, CAT, DIV, BEAUTIFY, PRE
from modules.helpers import TOTAL_ROWS

from DalView import *
from GrandTranslator import *
# from pydal._globals import DEFAULT

from helpers import get_fields_from_table_format, save_DAL_log, is_reference
from lib.w2ui import serialized_lists, make_orderby, save_export


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

    )

    mostly used for:
       form
       filter: query, having
       records_w2ui

    Explanation:
    Workflow:
       Search ->  Grid -> CRUD (CUD)

    """
    def __init__(self,
                 # columns=None,
                 grid_columns=None,
                 recid=None,

                 response_view = "plugin_AnySQLFORM/w2ui_grid.html",

                 # extra
                 cid=None,


                 # REFACTORed args
                    search = None,
                    grid = None,
                    crud = None,
                    dalview = None, # this is shared among search and grid: translator, join_chains, initial_query/having, groupby, etc...
                    auth = None,

                 # DALVIEW
                    dalview_translator = None, # TODO
                    # left_join_chains=None,  # probably would be enough (or [[]])
                    dalview_left_join_chains = None,
                    # table_name=None,
                    dalview_table_name = None,

                 # SEARCH
                    # search_fields = None,
                    search_fields=None,
                    search_fields_update_triggers=None,  # TODO
                    search_fast_filters = None,
                    search_form_factory = None,
                    search_use_grand_search_form = True,
                    search_formstyle=None,
                    search_field_decorator=SearchField,

                 # GRID
                 #    grid_columns = None,
                    grid_recid = None,
                    grid_columns_options = None, # dict: colname: { ..options..}
                    grid_columns_force_FK_table_represent=False,
                    # grid_options_sort = None,
                    # grid_options_extra = {},
                    grid_data_name=None,  # TODO auth_data_name ?

                 grid_w2ui_sort=None,
                 grid_w2ui_options_extra={},
                 grid_w2ui_options_extra_toolbar_extra=None,
                 # grid_function, --> crud_url_Records

                 # CRUD : actions/commands  Grid/Edit/Add/Import/...
                    crud_url_Records = None, # ex: grid_function
                    crud_url_Add = None,
                    crud_url_Edit = None,
                    crud_url_Import = None,
                    crud_controller=None,
                 # end REFACTORed args

                 **kwargs  # form_factory
                 ):

        request = current.request

        self.args = Storage() # for systematically storing args for: search, grid, cud contexts

        def get_arg( ctx_name, arg_name, locals_of_init=locals() ):
            """Gets argument by object in Register: "search", "grid" or "c(r)ud"
            and maps it to self.args.obj.attr
            context_arg --> self.args.context.arg
            search_fields  -->  self.args.search.fields
            grid_columns --> self.args.grid.columns

            The problem is, that (as the code changes) the <arg> can reside in:
              - prelisted args (context_arg=..)
              - **kwargs
              - <context>['arg']  -- overrides other values (if present)
              - # default of <context>['arg'] -- which is listed somewhere (probably will be in prelisted_args -- so no need to extra-check)...

            Behaves a bit like singleton, as it returns the figured out value..
            """

            full_arg_name = ctx_name + "_" + arg_name
            arg_val = None

            func = GrandRegister.__init__
            if full_arg_name in func.__code__.co_varnames[:func.func_code.co_argcount]:
                # print 'dbg locals', locals_of_init
                arg_val = locals_of_init[ full_arg_name ]
            elif full_arg_name in kwargs:
                arg_val = kwargs[full_arg_name] # TODO: maybe .pop()?
            elif arg_name in kwargs:
                arg_val = kwargs[arg_name]  # TODO: maybe .pop()?
                raise RuntimeError("Parameter %s=%r in kwargs (not in context args)"% (arg_name, arg_val))  # todo: later - warning would be enough

            self.args[ctx_name] = ctx_kwargs  =   locals_of_init[ctx_name] or self.args[ctx_name] or Storage()  # get from param or attr or new

            ctx_kwargs.setdefault( arg_name, arg_val) # if not given, set it

            return ctx_kwargs[arg_name]

        def prepare_context(ctx_name):
            func = GrandRegister.__init__
            arg_names_in_def = func.__code__.co_varnames[:func.func_code.co_argcount]
            for arg_name in     list(arg_names_in_def) + kwargs.keys():
                if arg_name .startswith(ctx_name+"_"):
                    arg_name_4ctx = arg_name[len(ctx_name)+1:]  # strip context name from it
                    get_arg(ctx_name, arg_name_4ctx)



            return self.args[ctx_name]

        # search = copy.copy(search)  # todo: maybe make shallow copy  -- not to pollute it outside of func...
        def __init__NG():
            for ctx in 'search grid crud dalview auth'.split():
                # self.args[ctx] = self.args[ctx] or Storage()
                prepare_context(ctx)

            self.args.dalview.translator = kwargs.get('translator') or get_arg('dalview', 'translator')


        __init__NG()

        # COLUMNS stuff
        self.columns = get_arg('grid', 'columns') or kwargs.get('columns')

        def init_table_name_and_recid():
            db = current.db

            main_table = None
            for col in self.columns:
                if hasattr(col, 'tablename'):
                    main_table = col.tablename
                    break

            self.table_name = self.args.dalview.table_name = get_arg('dalview', 'table_name') or main_table

            # record id field (or expression?)
            self.recid = get_arg('grid', 'recid')
            if not self.recid:
                main_table = get_arg('dalview', 'table_name')
                self.recid = db[main_table]._id
                print "DBG, recid", self.recid
                # self.recid = recid or columns[0].table._id # will be passed in w2ui grid to Edit/Delete
            else:
                if self.recid.tablename != self.table_name:
                    raise RuntimeError("recid   doesn't match  table_name:  %r   %r" % (self.recid, self.table_name) )

        init_table_name_and_recid()

        def init_w2ui_kwargs():
            # for w2ui_grid response_view
            self.w2ui_kwargs = Storage()
            self.w2ui_kwargs.cid           = self.cid           =  cid or request.function
            self.w2ui_kwargs.grid_function = self.grid_function =  get_arg('grid', 'function') or request.function # or get_arg('crud', 'url_Records') ?
            self.w2ui_kwargs.w2ui_sort = self.w2ui_sort =  get_arg('grid', 'w2ui_sort')

            # self.w2ui_kwargs.table_name    = self.table_name    =  table_name or  columns[0].tablename
            self.args.dalview.table_name = self.w2ui_kwargs.table_name    = self.table_name
            self.w2ui_kwargs.data_name     = self.data_name     =  get_arg('grid', 'data_name') or  self.table_name or request.controller
            # self.w2ui_kwargs.context_name  = self.context_name  =  kwargs.pop('context_name', self.data_name) # maybe unnecessary

            self.w2ui_kwargs.w2ui_options_extra = get_arg('grid', 'w2ui_options_extra')
            self.w2ui_kwargs.w2ui_options_extra_toolbar_extra = get_arg('grid', 'w2ui_options_extra_toolbar_extra')

        init_w2ui_kwargs()


        def init_crud_actions():
            self.w2ui_kwargs.crud_urls = self.crud_urls = Storage(kwargs.pop('crud_urls', {}))
            crud_controller = kwargs.pop('crud_controller', 'external')

            if crud_controller in ['postback', None]:
                self.crud_urls.setdefault('add', URL(args=['add']))
                self.crud_urls.setdefault('edit', URL(args=['edit', '___id___'],
                                                      vars={'view_extension': 'html'}))  # todo: use signature?

            else:
                self.crud_urls.setdefault('add', URL(crud_controller, 'add_' + crud_controller))
                self.crud_urls.setdefault('edit', URL(crud_controller, 'edit_' + crud_controller, args=['___id___']))

            if '___id___' not in self.crud_urls.edit:
                self.crud_urls.edit += '/___id___'  # though we risk in case of vars in URL

        init_crud_actions()



        # ordinary params

        self.response_view = response_view


        self.force_FK_table_represent = kwargs.get('force_FK_table_represent') or get_arg('grid', 'columns_force_FK_table_represent')
        if self.force_FK_table_represent:
            for nr, col in enumerate( self.columns ):
                if is_reference( col ):
                    self.columns[nr] = represent_FK( col )



        self.left_join_chains = dalview_left_join_chains or kwargs.get('left_join_chains') # probably would be enough

        # self.search_fields.append( SearchField('grid') )

        self.search_fields_update_triggers = search_fields_update_triggers

        self.search_fields = get_arg('search', 'fields') or search_fields
        self.args.search.pop('fields', None)  # # in form_factory they need to be separated form kwargs

        self.fast_filters = get_arg('search', 'fast_filters') or kwargs.get('filters') # actually they are addressed to grand_search_form

        self.kwargs = kwargs # TODO: dispach to contexts: search | grid | crud


        # CREATE SEARCH FORM

        self.use_grand_search_form = get_arg('search', 'use_grand_search_form')
        # print 'dbg use_grand_search_form', self.use_grand_search_form
        if self.use_grand_search_form:
            # kwargs.setdefault('form_factory', SQLFORM.factory) # TODO change to grand search_form..
            def my_grand_search_form(*fields, **kwargs):
                from searching import search_form as grand_search_form
                return grand_search_form(self.cid, *fields, **kwargs)

            # kwargs.setdefault( 'form_factory', my_grand_search_form )
            self.args.search.form_factory = my_grand_search_form

        # a bit smarter way -- in case   kwargs['form_factory'] is None
        # self.form_factory = kwargs.pop('form_factory', None) or  my_grand_search_form
        # kwargs['form_factory'] = self.form_factory


        # self.search_form = GrandSQLFORM( *self.search_fields, **kwargs ) # Deprecate
        # print 'dbg self.args.search', self.args.search
        self.search_form = GrandSQLFORM( *self.search_fields, **self.args.search )
        # self.search_fields = self.search_form.formfields_flat  # UPDATES items to flattened SearchField instances

        # self.search_fields_update_triggers                    # TODO: for ReactiveForm


    def w2ui_init(self):
        # some workarounds for grand core stuff
        # TODO: maybe refactor to separate W2ui_grid class?..

        response = current.response

        if self.response_view:
            response.view = self.response_view

        # response.subtitle = "test  w2ui_grid"
        # response.menu = response.menu or []

        self.columns.append( TOTAL_ROWS )

        self.w2ui_columns = []
        for f in self.columns:
            caption = getattr(f, 'label', str(f))
            try:
                if caption.lower() == f.table._id.name.lower(): caption = str(f).replace('.', ' ').title()
            except Exception as e: print "Warning in w2ui_init:", e

            w2ui_col =  {
                      'field': FormField(f).name,
                      'caption': caption,
                      'size': "100%",
                      'sortable': isinstance(f, (Field, Expression)) or hasattr(f, 'orderby'),
                      'resizable': True
                      }

            if hasattr(f, 'w2ui'):
                w2ui_col.update(f.w2ui)

            self.w2ui_columns.append( w2ui_col )


        self.w2ui_colnames = [d['field'] for d in self.w2ui_columns]  # parallely alligned to columns
        self.colnames = [ str(col) for col in self.columns ]          # parallely alligned to columns

        self.map_w2uinames_2_columns = dict(zip(self.w2ui_colnames, self.columns))
        
        self.map_colnames_2_w2uinames = dict(zip( self.colnames, self.w2ui_colnames))

        if getattr(self, 'w2ui_sort', None) is None: # if not set or set to None
            self.w2ui_sort = [{'field': self.w2ui_colnames[0] }]

        for sorter in self.w2ui_sort:
            sorter.setdefault('direction', "asc")

        # self.w2ui_kwargs['w2grid_options_extra']= dict(autoLoad=False ) # dbg

        context = dict(
            w2ui_columns=self.w2ui_columns[:-1],  # remove TOTAL_ROWS from end...
            # w2ui_sort=self.w2ui_sort ,  # w2ui_sort = [  {'field': w2ui_colname(db.auth_user.username), 'direction': "asc"} ]

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
        context = self.w2ui_init()
        context['form'] =  self.search_form

        # for dbg purposes
        if not self.use_grand_search_form:

            ajax_url = "javascript:ajax('%s', %s, 'grid_records'); " % (
                URL(vars=dict(_grid=True, _grid_dbg=True), extension=None),
                [f.name for f in self.search_form.formfields_flat]
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
            cmd = request.vars.cmd

            status = 'success'
            if cmd in ('get-records', 'export-records'):
                if not current.auth.has_permission('list', 'warehouse_batch'):
                    return response.json( {'status': 'error', 'message': current.MSG_NO_PERMISSION + ": %s %s %s" %('delete', self.table_name, current.auth)}  )


                records = self.w2ui_grid_records()

                if cmd == 'get-records':

                    TOTAL_ROWS_w2i = self.map_colnames_2_w2uinames[TOTAL_ROWS]
                    total = records[0][TOTAL_ROWS_w2i] if records else 0
                    result =  {'status': status, 'total': total, 'records': records}

                    # if getattr(current, 'DBG', False):
                    #     save_DAL_log()

                    return  response.json( result )

                elif cmd == 'export-records':
                    selected_w2ui_colnames = request.vars.getlist('columns[]')
                    # TODO FIXME: optimize -- in DB query limit the selected columns!
                    selected_w2ui_columns = [c for c in self.w2ui_columns if c['field'] in selected_w2ui_colnames]

                    data = [ [col['caption'] for col in selected_w2ui_columns ] ]
                    for r in records:
                        data.append([r[ colname ] for colname in selected_w2ui_colnames])

                    save_export(self.cid, data)
                    return response.json( {'status': status} )

                elif cmd == 'delete-records':
                    if not current.auth.has_permission('delete', self.table_name):
                        return {'status': 'error', 'message': current.MSG_NO_PERMISSION + ": %s %s %s" %('delete', self.table_name, current.auth) }

                    selected = request.vars.getlist('selected[]')
                    try:
                        for s in selected:
                            del db[self.table_name][s]  # TODO: optimize with belongs..
                            # delete_field_translations(db, db[self.table_name], rid=s) # TODO: if needed

                            # from some use-case..
                            # record = db.purchase_order(s)
                            # if _is_deletable(record):
                            #     del db.purchase_order[s]
                            #     log_changes(db, 'purchase_order', s, old_record=record,
                            #                 label='purchase_order__delete_form',
                            #                 fields=['number'])
                    except:
                        db.rollback()
                        return response.json( {'status': 'error', 'message': current.MSG_ACTION_FAILED} )

                    return response.json( {'status': status} )


            # modifying actions
            elif request.args(0) in ['add', 'edit']:  # default CRUD actions
                action = request.args(0)
                table = self.recid.table
                if action=='add':
                    form = SQLFORM(table)
                    form.process()
                    raise HTTP(200, form=form)  # for ajax request

                if action=='edit':
                    view_extension = request.vars.view_extension
                    record = table(request.args(1)) # or redirect(..)
                    form = SQLFORM( table , record )
                    form.process()

                    return response.render('core/base_form.html', dict(form=form, row_buttons=None)) # for full request
                    # raise HTTP(200, response.render('core/base_form.html', dict(form=form, row_buttons=None))) # for full request

            # if form.process().accepted:
            #     response.flash = 'form accepted'
            # elif form.errors:
            #     response.flash = 'form has errors'

        # default initial register (search_form + grid)
        else:
            # raise HTTP(200, response.render(self.response_view, self.form() ) )
            return  response.render(self.response_view, self.form() ) # ?


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

    def w2ui_get_orderby(self):
        request = current.request
        db = current.db
        extra = serialized_lists(request.vars)  # sort, search

        if 'sort' in extra:

            fields_mapping = {}
            for w2ui_name in self.w2ui_colnames:
                column = self.map_w2uinames_2_columns[ w2ui_name ]
                if hasattr(column, 'orderby'):
                    fields_mapping[w2ui_name] = column.orderby  # for virtrual fields
                elif isinstance(column, Field.Virtual) and hasattr(column, 'required_fields'):
                    # construct order by -- putting id/ref fields at the end..
                    id_fields = []
                    order_fields = []
                    for f in column.required_fields:
                        if is_reference(f) or str(f)==str(f.table._id) :
                            id_fields.append(f)
                        else:
                            order_fields.append(f)
                    order_fields.extend( id_fields )
                    column.orderby = reduce(lambda a, b: a|b, order_fields)

                else:
                    fields_mapping[w2ui_name] = column

            orderby = make_orderby(db, extra['sort'], fields_mapping=fields_mapping, table_name=self.recid.tablename) # table_name=None, append_id=False

            return orderby


    def select(self):
        """get selection by filter of current request """

        request = current.request

        filter = self.search_form.build_queries()


        if request.vars.cmd == 'get-records':
            offset = int(request.vars.offset)
            limit = int(request.vars.limit)
            limitby = (offset, offset + limit)
        else:
            limitby = None


        # self.selection = DalView(
        rows = grand_select(
                        self.recid, *self.columns,
                        query=filter.query, having=filter.having,

                         left_join_chains=self.left_join_chains,
                         # group order distinct
                         orderby=self.w2ui_get_orderby(),
                         limitby=limitby,
                         **self.kwargs # translator inside
                         )

        if hasattr(current, 'DBG') and current.DBG:
            # from helpers import tidy_SQL
            # current.session.sql_log = map(tidy_SQL, rows.sql_log)
            current.session.sql_log =  PRE( rows.sql_log )
            current.session.sql_log_nontranslated = PRE( rows.sql_nontranslated_log )

        # rows = self.selection.execute()

        return rows

    def w2ui_grid_records(self):
        self.w2ui_init()

        # in real usecase - we want to RENDER first
        def rows_rendered_flattened(rows, fk_fields_leave_int=True):
            if not rows: return [] # if empty
            colnames = rows.colnames
            # _compact = rows.compact
            rows.compact = False

            from helpers import force_refs_represent_ordinary_int
            force_refs_represent_ordinary_int(rows)  # otherwise rows.render() would call extra selects for each row for each FK


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

        # map_colnames_2_w2ui = dict( zip(self.colnames, self.w2ui_colnames ) )

        records =  [ ]

        recid_str = str(self.recid) # add w2ui recID

        for row in rows:
            # r = {  map_colnames_2_w2ui[colname]:  row[ colname ]       for colname in self.colnames }
            r = {  self.map_colnames_2_w2uinames[colname]:  row[ colname ]       for colname in self.colnames    }
            r['recid'] = row[recid_str]
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
        pass



    def set_default_validator(self, f):


        if f.override_validator==False:  # do not override
            return

        if not self.translator:
            QuerySQLFORM.set_default_validator(self, f)
            return

        if self.translator.is_validator_translated(f.requires):  # if already translated
            return



        db = current.db  # todo: for Reactive form should be prefiltered dbset
        target = f.target_expression  # for brevity

        if f.type.startswith('reference ') or f.type.startswith('list:reference '):
            if not f.requires or f.requires == DEFAULT or f.override_validator:
                foreign_table = f.type.split()[1]
                foreign_table = foreign_table.split(':')[-1] # get rid of possible "list:"
                # f.requires = self.default_IS_IN_DB(db, db[foreign_table], db[foreign_table]._format)


                format = db[foreign_table]._format
                fields_in_format =    get_fields_from_table_format(format)
                fields_in_format_as_str =  [ str(db[foreign_table][fname])    for fname in    fields_in_format]
                # apply str as without it fields comparison allways gives True..?
                if set(fields_in_format_as_str ) & set( map(str, self.translator.fields) ): # if there are translatable fields in format
                    f.requires = T_IS_IN_DB(self.translator, db, db[foreign_table], format, multiple=f.multiple)
                    return


        # if field needs to be translated
        if str(target) in map(str, self.translator.fields):

            if  isinstance(target, Field) :
                if f.type in ('string', 'text'):  # maybe also number type? or list:string

                    if f.comparison in [ 'equal', 'belongs']:
                        f.requires = T_IS_IN_DB(self.translator, db, target, multiple=f.multiple, distinct=True)

                    if f.comparison == 'contains':
                        # f.requires = IS_IN_DB(db, target)
                        # http://web2py.com/books/default/chapter/29/07/forms-and-validators#Autocomplete-widget
                        # f.widget = SQLFORM.widgets.autocomplete(
                        f.widget = T_AutocompleteWidget( self.translator,
                            current.request,
                            target,
                            distinct=True,
                            # , keyword='_autocomplete_%(tablename)s_%(fieldname)s__'+f.name # in case there would be 2 same targets
                            # , recid=db.category.id
                        )

            if type(target) is Expression  and f.comparison  in ['equal', 'belongs']:
            # should work for Expression targets  -- and (seems) doesn't depend on translation
                target = f.target_expression
                # theset = db(target._table).select(target).column(target)
                theset = DalView(target, distinct=target, translator=self.translator).execute().column(target)
                f.requires = IS_IN_SET(theset, multiple=f.multiple)


        else:  # for Field targets...
            QuerySQLFORM.set_default_validator(self, f)



def grand_select(*args, **kwargs):
    return select_with_virtuals(*args, **kwargs)
    # return DalView(*args, **kwargs).execute()
