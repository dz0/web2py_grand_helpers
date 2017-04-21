# -*- coding: utf-8 -*-

# from gluon import current
from plugin_AnySQLFORM import *
from gluon.html import URL, A, CAT, DIV, BEAUTIFY, PRE
from modules.helpers import TOTAL_ROWS

from plugin_DalView import *
from plugin_GrandTranslator import *
# from pydal._globals import DEFAULT

from plugin_grand_helpers import get_fields_from_table_format, save_DAL_log, is_reference, update_dict_override_empty, join_dicts, test_fields, get_distinct
from lib.w2ui import serialized_lists, make_orderby, save_export

        
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
    Usual workflow in register:
       Search form ->  Grid data -> CRUD actions

    Note: most construcor parameters are grouped into contexts: search_..,  dalview_, w2ui_.., crud_.. 

    should look sth like:
    >>> GrandRegister(
        columns = [.. ],
        search_fields = [ ..],
        dalview_left_join_chains = [ [..] ], # can be used by both grid and search_form (if reactive)        #probably left is enough?
        dalview_translator=GrandTranslator( <fields>, <language_id>)
        #, search_fields_update_triggers = {None:[ ]}
    )

    """
    contexts = 'search w2ui crud dalview'.split()
    def __init__(self,
                 columns=None,
                 cid=None,  # usually request.function
                 maintable_name=None,  # mostly needed for recid while migrating from oldschool :  w2ui_w2ui_coldata_oldschool_js
                 columns_force_FK_table_represent=False,

                 response_view = "plugin_GrandRegister/w2ui_grid.html",

                 # Contexts of args
                    search = None,
                    w2ui = None,
                    crud = None,
                    dalview = None,  # this is shared among search and grid: translator, join_chains, initial_query/having, groupby, etc...

                 # DALVIEW
                    dalview_translator = None,  # TODO
                    # left_join_chains=None,  # probably would be enough (or [[]])
                    dalview_left_join_chains = None,
                    dalview_left=None,

                 # SEARCH
                    # search_fields = None,
                     search_fields=None,
                     search_fields_update_triggers=None,  # TODO
                     search_fast_filters = None,
                     search_form_factory = None,
                     search_use_grand_search_form = True,
                     search_formstyle=None,
                     search_field_decorator=SearchField,

                 # W2UI
                     w2ui_sort=None,
                     w2ui_gridoptions_oldschool_js=None,  # ex w2ui_coldata_oldschool_js
                     w2ui_options_more={},
                     w2ui_extra_toolbar_more=None,

                 # CRUD : actions/commands  Edit/Add/Import/... # if given, used in w2ui options
                     crud_controller=None,  # in case it is different from current
                     crud_data_name=None, # usually is singular form of last word (after underscore) in  cid or maintable_name # defaults to cid with rtirm("s")
                     crud_form_urls=None,  # add/edit/import
                     # crud_urls=None,  # todo: deprecate

                 **kwargs  # just in case
                 ):

        request = current.request

        self.vars = self.args = Storage() # for systematically storing args for: search, grid, cud contexts

        def get_arg( ctx_name, arg_name, locals_of_init=Storage( locals() ) ):
            """Gets argument by object in Register: "search", "grid" or "c(r)ud"
            and maps it to self.obj.attr
            context_arg --> self.context.arg
            search_fields  -->  self.search.fields
            columns --> self.columns

            The problem is, that (as the code changes) the <arg> can reside in:
              - prelisted args (context_arg=..)
              - **kwargs
              - <context>['arg']  -- overrides other values (if present)

            Behaves a bit like singleton, as it returns the figured out value..
            """

            if ctx_name: full_arg_name = ctx_name + "_" + arg_name
            else: full_arg_name = arg_name

            arg_val = None

            func = GrandRegister.__init__
            if full_arg_name in func.__code__.co_varnames[:func.func_code.co_argcount]:
                # print 'dbg locals', locals_of_init
                arg_val = locals_of_init[ full_arg_name ]
            elif full_arg_name in kwargs:
                arg_val = kwargs[full_arg_name] # TODO: maybe .pop()?

            def extra_caution(arg_val):
                # check if param is not "orphaned" (name provided without context)
                # or if it is not in wrong context
                if ctx_name:
                    # check orphaned
                    if arg_name in kwargs:
                        arg_val = kwargs[arg_name]  # TODO: maybe .pop()?
                        raise RuntimeError("Parameter %s=%r in kwargs (not in context ('%s_') args)"% (arg_name, arg_val, ctx_name))
                    if arg_name in list( func.__code__.co_varnames[:func.func_code.co_argcount]):
                        arg_val = locals_of_init[ arg_name ]
                        raise RuntimeError("Parameter %s=%r in standart params (not in context ('%s_') args)"% (arg_name, arg_val, ctx_name))

                # check wrong context
                for other_ctx_name in  GrandRegister.contexts:
                    if other_ctx_name != ctx_name:
                        other_full_arg_name = other_ctx_name + "_" + arg_name
                        if other_full_arg_name  in kwargs:
                            arg_val = kwargs[other_full_arg_name ]
                            raise RuntimeError("Parameter %s=%r in other context ('%s_) kwargs (not in context (%r) args)"% (arg_name, arg_val, other_ctx_name, ctx_name))
                        if other_full_arg_name  in list( func.__code__.co_varnames[:func.func_code.co_argcount]):
                            arg_val = locals_of_init[ other_full_arg_name  ]
                            raise RuntimeError("Parameter %s=%r in other context ('%s_) in standart params (not in context (%r) args)"% (arg_name, arg_val, other_ctx_name, ctx_name))


                # a way to track deprecated args
                deprecated_args = 'recid'.split()
                deprecated_ctxs = 'grid'.split()
                if arg_name in deprecated_args :
                    raise RuntimeError("Deprecated arg %s=%r (in context ('%s_') args)" % (arg_name, arg_val, ctx_name))
                if ctx_name in deprecated_ctxs:
                    raise RuntimeError("Deprecated context %r with arg %s=%r." % (ctx_name, arg_name, arg_val))

            extra_caution(arg_val)

            if ctx_name:
                ctx_kwargs  =   locals_of_init[ctx_name] or getattr(self, ctx_name, None)  or Storage()  # get from param or attr or new
                ctx_kwargs.setdefault( arg_name, arg_val) # if not given, set it
                setattr(self, ctx_name, ctx_kwargs)   # set context it as Register attribute
                return ctx_kwargs[arg_name]

            else:
                setattr(self, arg_name, arg_val)
                return arg_val



        def prepare_context(ctx_name):
            func = GrandRegister.__init__
            arg_names_in_def = func.__code__.co_varnames[1:func.func_code.co_argcount]  # skip 0-th ("self")

            for arg_name in     list(arg_names_in_def) + kwargs.keys():
                if ctx_name:
                    if arg_name .startswith(ctx_name+"_"):
                        arg_name_4ctx = arg_name[len(ctx_name)+1:]  # strip context name from it
                        print 'dbg ctx arg:', ctx_name, arg_name_4ctx
                        get_arg(ctx_name, arg_name_4ctx)
                else:
                    if arg_name.split('_')[0] not in GrandRegister.contexts:
                        print 'dbg ctx arg:', ctx_name, arg_name
                        get_arg(None, arg_name)


            return self.args[ctx_name]

        # search = copy.copy(search)  # todo: maybe make shallow copy  -- not to pollute it outside of func...
        # POPULATE CONTEXTS with args
        for ctx in GrandRegister.contexts+[None]:
            # self.args[ctx] = self.args[ctx] or Storage()
            prepare_context(ctx)



        self.columns = columns
        self.kwargs = kwargs # kwargs of init



        # COLUMNS stuff

        if self.columns and not self.w2ui.gridoptions_oldschool_js:
            def init_maintable_name_and_recid():
                # if not self.columns:     return
                db = current.db

                def find_maintable_name():
                    for col in self.columns:
                        if hasattr(col, 'tablename'):
                            return col.tablename

                # if not given, try to figure maintable from columns (todo: from search_fields)
                self.maintable_name = self.maintable_name  or find_maintable_name()

                if self.maintable_name:
                    self.recid = db[self.maintable_name]._id
                else:
                    # trigger ExceptionWarning
                    self.recid = get_arg('grid', 'recid')

            init_maintable_name_and_recid()

            # self.columns_force_FK_table_represent = kwargs.get('columns_force_FK_table_represent') or get_arg('grid', 'columns_force_FK_table_represent')
            if self.columns_force_FK_table_represent:
                for nr, col in enumerate( self.columns ):
                    if is_reference( col ):
                        self.columns[nr] = represent_FK( col )

            #TODO: maybe move TOTALROWS column addition here
        else:
            self.recid = kwargs['recid_4oldschool']
        # ordinary params

        self.response_view = response_view



        # for backward compatibility
        # self.left_join_chains = dalview_left_join_chains or kwargs.get('left_join_chains') # probably would be enough
        self.left_join_chains = self.dalview.left_join_chains
        self.left = self.dalview.left
        self.translator = self.dalview.translator

        self.kwargs = kwargs # TODO: dispach to contexts: search | grid | crud




    def w2ui_init(self):

        response = current.response

        if self.response_view:
            response.view = self.response_view

        # response.subtitle = "test  w2ui_grid"
        # response.menu = response.menu or []


        if not self.w2ui.gridoptions_oldschool_js and self.columns : # backwards compatibility (while migrating) one can use oldschool defs taken from  templates (can be rendered with  gluon.template.render(content='...', context=<vars>)

            self.columns.append(TOTAL_ROWS)  #

            def w2ui_init_columns():
                self.w2ui.columns = []
                for f in self.columns:

                    caption = getattr(f, 'label', str(f))
                    try:
                        if caption.lower() == f.table._id.name.lower(): caption = str(f).replace('.', ' ').title()
                    except Exception as e: print "Warning in w2ui_init:", e

                    # defaults
                    w2ui_col =  {
                              # 'field': FormField(f).name,
                              'field': FormField.construct_new_name( f),
                              'caption': caption,
                              'size': "100%",
                              'sortable': isinstance(f, (Field, Expression)) or hasattr(f, 'orderby'),
                              'resizable': True
                            # 'render': ?
                            # 'hidden': ?
                              }
                    # override defaults or include extra options: render, hidden...
                    if hasattr(f, 'w2ui'):
                        w2ui_col.update(f.w2ui)

                    self.w2ui.columns.append( w2ui_col )

                self.w2ui.colnames = [d['field'] for d in self.w2ui.columns]  # parallely alligned to columns
                self.colnames = [ str(col) for col in self.columns ]          # parallely alligned to columns

                self.map_w2uinames_2_columns = dict(zip(self.w2ui.colnames, self.columns))
                self.map_colnames_2_w2uinames = dict(zip( self.colnames, self.w2ui.colnames))

                if self.w2ui.sort is None: # if not set or set to None
                    self.w2ui.sort = [{'field': self.w2ui.colnames[0] }]

                for sorter in self.w2ui.sort:
                    sorter.setdefault('direction', "asc")

                del self.w2ui.columns[-1]  # remove TOTAL_ROWS from end...

            w2ui_init_columns()

        def w2ui_init_crud_actions():
            crud = self.crud

            if crud.data_name is None:
                crud.data_name = self.cid or self.maintable_name

                # primitive hack to make singular form
                crud.data_name = crud.data_name.rstrip('s')
                if crud.data_name.endswith('ie'): crud.data_name=crud.data_name[:-2]+'y' # categorie --> category


            if not crud.form_urls: crud.form_urls = {}

            if crud.controller is None:  # means self
                crud.form_urls.setdefault('add', URL(args=['add']))
                crud.form_urls.setdefault('edit', URL(args=['edit', '___id___'],
                                                      vars={'view_extension': 'html'}))  # todo: use signature?
                crud.form_urls.setdefault('import', URL(args=['import']))

            else:
                # try to guess names
                crud.form_urls.setdefault('add', URL(crud.controller, 'add_' + crud.data_name))
                crud.form_urls.setdefault('edit', URL(crud.controller, 'edit_' + crud.data_name, args=['___id___']))
                crud.form_urls.setdefault('import', URL(crud.controller, 'import_' + crud.data_name))

            crud.form_urls = Storage( crud.form_urls  )

            if '___id___' not in crud.form_urls.edit:
                crud.form_urls.edit += '/___id___'  # though we risk in case of vars in URL

        w2ui_init_crud_actions()


        context = dict(
            cid=self.cid,
            maintable_name=self.maintable_name,
            w2ui = self.w2ui,
            # crud = self.crud
        )

        join_dicts(context, self.crud, result_as_new=False)

        return context

    def search_form_init(self):
        # CREATE SEARCH FORM

        # print 'dbg use_grand_search_form', self.use_grand_search_form
        if self.search.use_grand_search_form:
            # kwargs.setdefault('form_factory', SQLFORM.factory) # TODO change to grand search_form..
            def my_grand_search_form(*fields, **kwargs):
                from searching import search_form as grand_search_form
                return grand_search_form(self.cid, *fields, **kwargs)

            # kwargs.setdefault( 'form_factory', my_grand_search_form )
            self.search.form_factory = my_grand_search_form

        # a bit smarter way -- in case   kwargs['form_factory'] is None
        # self.form_factory = kwargs.pop('form_factory', None) or  my_grand_search_form
        # kwargs['form_factory'] = self.form_factory


        search_fields = self.search.pop('fields', None)  # in form_factory they need to be separated form kwargs

        self.search_form = GrandSQLFORM( *search_fields, **join_dicts(self.search, self.dalview) )

        self.search.fields = search_fields  # put them back (just in case :))

        # self.search_fields = self.search_form.formfields_flat  # UPDATES items to flattened SearchField instances

        # self.search_fields_update_triggers                    # TODO: for ReactiveForm

    def search_form_nongrand_inject_ajax(self, context):
        # for dbg purposes
        # if not self.search.use_grand_search_form:

            ajax_url = "javascript:ajax('%s', %s, 'w2ui_records'); " % (
                URL(vars=dict(_grid=True, _w2ui_dbg=True), extension=None),
                [f.name for f in self.search_form.formfields_flat]
            )

            ajax_link = A('ajax load records', _href=ajax_url, _id="ajax_loader_link")
            ajax_result_target = DIV( "...AJAX RECORDS target...", _id='w2ui_records')

            context['form'] = CAT(ajax_result_target, ajax_link,
                                  context['form'] ,
                                  CAT("fast_filters", self.search.fast_filters)
                                  )

            return context


    def render(self, cmd=None):  # TODO: maybe call "action()"
        """dispached rendering/results:
              either generate initial register
              or get/export/delete records,
              or add/edit/import   form/action
        """

        request = current.request
        response = current.response

        if request.vars._grid:  # if it is postback from register
            cmd = request.vars.cmd or cmd # priority for vars

            status = 'success'
            if cmd in ('get-records', 'export-records'):
                if not current.auth.has_permission('list', 'warehouse_batch'):
                    return response.json( {'status': 'error', 'message': current.MSG_NO_PERMISSION + ": %s %s %s" %('delete', self.maintable_name, current.auth)}  )


                records = self.data_render_records()

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
                    selected_w2ui_columns = [c for c in self.w2ui.columns if c['field'] in selected_w2ui_colnames]

                    data = [ [col['caption'] for col in selected_w2ui_columns ] ]
                    for r in records:
                        data.append([r[ colname ] for colname in selected_w2ui_colnames])

                    save_export(self.cid, data)
                    return response.json( {'status': status} )

                elif cmd == 'delete-records':
                    if not current.auth.has_permission('delete', self.maintable_name):
                        return {'status': 'error', 'message': current.MSG_NO_PERMISSION + ": %s %s %s" %('delete', self.maintable_name, current.auth) }

                    selected = request.vars.getlist('selected[]')
                    try:
                        for s in selected:
                            del db[self.maintable_name][s]  # TODO: optimize with belongs..
                            # delete_field_translations(db, db[self.maintable_name], rid=s) # TODO: if needed

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
            # elif request.args(0) == 'crud_form':  # todo: apply API change
            #     request.args.pop(0)
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
            context = self.w2ui_init()
            self.search_form_init()
            context['form'] = self.search_form

            # for debug purposes
            if not self.search.use_grand_search_form:
                self.search_form_nongrand_inject_ajax(context)

            return  response.render(self.response_view, context ) # ?


    # def search_filter(self):
    #     " query and having "
    #     return self.search_form().build_queries()


    # # SIMPLE DATA -- ok for w2ui_w2ui_records
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

    def data_get_w2ui_orderby(self):
        request = current.request
        db = current.db
        extra = serialized_lists(request.vars)  # sort, search

        if 'sort' in extra:

            fields_mapping = {}
            for w2ui_name in self.w2ui.colnames:
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

            orderby = make_orderby(db, extra['sort'], fields_mapping=fields_mapping, table_name=self.recid.tablename) # maintable_name=None, append_id=False

            return orderby


    def data_select(self):
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
        # rows = grand_select( -- deprecate grand_select naming
        rows = select_with_virtuals(
                        self.recid, *self.columns,
                        query=filter.query, having=filter.having,

                         # group order distinct
                         orderby=self.data_get_w2ui_orderby(),
                         limitby=limitby,
                         # **self.kwargs # translator inside
                         # left_join_chains=self.left_join_chains,
                         **self.dalview # translator and left and left_join_chains inside
                         )

        if hasattr(current, 'DBG') and current.DBG:
            # from helpers import tidy_SQL
            # current.session.sql_log = map(tidy_SQL, rows.sql_log)
            current.session.sql_log =  PRE( rows.sql_log )
            current.session.sql_log_nontranslated = PRE( rows.sql_nontranslated_log )

        # rows = self.selection.execute()

        return rows

    def data_render_records(self):
        """the "data_grid" function  analogue:
         1) builds query from search_form ,
         2) fetches data,
         3) renders it (applies represent)
         4) converts data to w2grid format (flattens Row structure, applies w2grid naming)
         """

        self.search_form_init() # prepare stuff to get query (filters)
        self.w2ui_init()   # prepare stuff to get columns

        # in real usecase - we want to RENDER first
        def rows_rendered_flattened(rows, fk_fields_leave_int=True):
            if not rows: return [] # if empty
            colnames = rows.colnames
            # _compact = rows.compact
            rows.compact = False

            from plugin_grand_helpers import force_refs_represent_ordinary_int
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
        initial_rows = self.data_select()

        # map to w2ui colnames

        rows =  rows_rendered_flattened(initial_rows)


        # map colnames to w2ui fieldnames # TODO: could be done in "flattening" step?
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

        # records = as_htmltable(records, self.w2ui.colnames) # for testing

        return records




class GrandSQLFORM(SearchSQLFORM):
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

        kwargs.setdefault('maintable_name', 'GrandSQLFORM')
        SearchSQLFORM.__init__(self, *fields, **kwargs)
        pass



    def set_default_widget(self, f):


        if f.override_widget==False:  # do not override
            return

        if not self.translator:
            SearchSQLFORM.set_default_widget(self, f)
            return

        if self.translator.is_validator_translated(f.requires):  # if already translated
            return

        if self.translator.is_widget_translated(f.widget):
            return


        db = current.db  # todo: for Reactive form should be prefiltered dbset
        target = f.target_expression  # for brevity

        if f.type.startswith('reference ') or f.type.startswith('list:reference '):
            if not f.requires or f.requires == DEFAULT or f.override_widget:
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
            distinct=get_distinct(target)
            if  isinstance(target, Field) :
                if f.type in ('string', 'text'):  # maybe also number type? or list:string

                    if f.comparison in [ 'equal', 'belongs']:
                        f.requires = T_IS_IN_DB(self.translator, db, target, multiple=f.multiple, distinct=distinct)

                    if f.comparison == 'contains':
                        # f.requires = IS_IN_DB(db, target)
                        # http://web2py.com/books/default/chapter/29/07/forms-and-validators#Autocomplete-widget
                        # f.widget = SQLFORM.widgets.autocomplete(
                        f.widget = T_AutocompleteWidget( self.translator,
                            current.request,
                            target,
                            distinct=distinct,
                            # , keyword='_autocomplete_%(tablename)s_%(fieldname)s__'+f.name # in case there would be 2 same targets
                            # , recid=db.category.id
                        )

            if type(target) is Expression  and f.comparison  in ['equal', 'belongs']:
            # should work for Expression targets  -- and (seems) doesn't depend on translation
                target = f.target_expression
                # theset = db(target._table).select(target).column(target)
                theset = DalView(target, distinct=distinct, translator=self.translator).execute().column(target)
                f.requires = IS_IN_SET(theset, multiple=f.multiple)


        else:  # for Field targets...
            SearchSQLFORM.set_default_widget(self, f)



# def grand_select(*args, **kwargs):
#     return select_with_virtuals(*args, **kwargs)
    # return DalView(*args, **kwargs).execute()
