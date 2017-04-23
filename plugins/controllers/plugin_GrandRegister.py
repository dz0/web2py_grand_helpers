from plugin_GrandRegister import GrandRegister, select_with_virtuals
from plugin_GrandRegister import GrandTranslator, T_IS_IN_DB, GrandSQLFORM
from plugin_grand_helpers import test_fields, get_expressions_from_formfields
from plugin_AnySQLFORM import FormField, SearchField
from plugin_DalView import DalView
from plugin_joins_builder import build_joins_chain



def test_40_GrandForm():
    fields = test_fields()

    fields[0].comparison = 'equal'
    a, b, c, d = fields[:4]
    fields = [ [a, b], [c, d] ]  # prepare for solidform

    def my_grand_search_form(*fields, **kwargs):
        from searching import search_form as grand_search_form
        return grand_search_form('test', *fields, **kwargs)


    form = GrandSQLFORM(*fields

                       , form_factory=my_grand_search_form
                        , formstyle =  None #'table3cols' or 'divs' # if not defined -- 'table2cols'
                        )

    data = form.vars_as_Row()

    return dict(form=form)
    return form

def test_41_grandregister_form_and_ajax_records_ERROR_possibly_permanent_loop(  ): # FIXME
    search_fields = test_fields()
    cols = get_expressions_from_formfields(search_fields )

    register = GrandRegister(cols,
                             cid = 'w2ui_test', # w2ui
                             auth_data_name = 'test grand',
                             dalview_table_name = 'test_grand',
                             search_fields = search_fields ,
                             dalview_left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                             # , response_view = None
                             , dalview_translator = gt
                             , search_use_grand_search_form = False
                             )

    # response.view = ...
    if request.vars._grid:
        rows = register.data_render_records()

        # return BEAUTIFY(  [ filter, rows ]  )  # for testing

        if request.vars._grid_dbg:
            def as_htmltable(rows, colnames):
                from gluon.html import TABLE
                return TABLE([colnames] + [[row[col] for col in colnames] for row in rows])
            rows = as_htmltable(rows, [FormField(col).name for col in register.columns]) # for testing

        # from gluon.serializers import json
        # return json(dict(status='success', records = rows ))
        response.view = "generic.json"
        return dict(status='success', records = rows )  # JSON

        # return DIV( filter, register.records_w2ui() )
        # return dict(records=register.records_w2ui())

    else:
        # response.view = "plugin_w2ui_grid/w2ui_grid.html"
        # register.search_form.      add_button( 'grid', URL(vars=dict(grid=True)))
        register.search_form_init()
        result = register.search_form

        # for debug purposes:
        # tablename = register.search_form.table._tablename
        ajax_url = "javascript:ajax('%s', %s, 'grid_records'); " % (
                                                URL(vars=dict(_grid=True, _grid_dbg=True), extension=None)  ,
                                                [f.name for f in register.vars.search.fields] if register.vars.search else None
                   )

        ajax_link = A('ajax load records', _href=ajax_url, _id="ajax_loader_link")
        ajax_result_target = DIV(BEAUTIFY(register.data_render_records()), _id='grid_records')
        # register.search_form.      add_button( 'ajax load records', ajax_url )
        # result['ajax_records']=
        # result['ats']=ajax_result_target

        # register.search_form[0].insert(0, ajax_link)
        result['form'] = CAT(ajax_result_target , ajax_link, result['form'] )


        return result




# SWITCH to use searchform with SOLIDFORM.factory  and fast_filters
use_grand_search_form=True # default is True (and needs test_app)

def test_49_TODO_grandregister_autocomplete_breaks_fastfilters_for_same_field():
    pass
    #TODO

def test_47_grandregister():
    search_fields = test_fields() [:4]
    cols = get_expressions_from_formfields(search_fields )

    from plugin_GrandRegister import create_fast_filters, SearchField
    sf = search_fields[0]
    # fast_filters = create_fast_filters(  sf ) # without translations  # TODO test more thoroughly
    translated_values = DalView( sf, distinct=True, translator = gt ).execute().column( sf )
    fast_filters = create_fast_filters( sf, values=translated_values )  # TODO test more thoroughly

    if use_grand_search_form:
        # fast filters
        # f = search_fields[0]
        # sf = SearchField( f )
        # fast_filters = [  {'label': T('core__all'), 'selected': True, 'data': {}},  ]
        # for row in db(f).select():
        #     fast_filters.append({'label': row[f.name], 'data': {sf.name: row[f.name]}})


        # for SOLIDFORM
        a, b, c, d =  search_fields
        search_fields_structured = [   [a, b],    [c, d]    ]

        search_kwargs = dict( search_fields=search_fields_structured  #,  filters=fast_filters
                       #, response_view = "" # "plugin_AnySQLFORM/w2ui_grid.html"
                     )
    else:
        # kwargs = dict( search_fields=search_fields,   use_grand_search_form=False        )
        search_kwargs = dict( search_fields=search_fields,   search_use_grand_search_form=False        )


    register = GrandRegister(cols,
                             cid='w2ui_test', # w2ui
                             # maintable_name = 'test_grand',

                             # left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                             dalview_left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                             # , response_view = None
                             # , translator=gt
                             , dalview_translator=gt

                             # , filters = fast_filters
                             , search_fast_filters = fast_filters
                             # , formstyle =  None  or 'table3cols' # divs table2cols table3cols
                             # , search_formstyle =  None  or 'table2ols' # divs table2cols table3cols
                             , **search_kwargs  # SEARCH FIELDS, etc
                             # , **kwargs  # SEARCH FIELDS, etc
                             )
    return register.render()

test_grandregister = test_47_grandregister
def test_42_grandregister_with_just_SQLFORM__PROBLEM(): #FixMe: ajax response "None":/
    global use_grand_search_form
    use_grand_search_form = False
    return test_grandregister()



def test_60_subjects_country_joins_chain():
    join_expr = build_joins_chain( db.subject_subject, db.subject_address,  db.address_address, db.address_country )
    # print join_expr
    # rows = db().select(db.subject_subject.title,  db.address_country.title, left=join_expr, limitby=(0,30))
    rows = select_with_virtuals(db.subject_subject.title,  db.address_country.title, left=join_expr, limitby=(0,30))

    return dict( rows=rows, sql=db._lastsql, dbg=response.toolbar() )
    # join_expr= build_joins_chain( db.subject_subject, db.subject_address, db.address_address, db.address_country)
    # return str(join_expr)

from plugin_DalView import represent_PK, represent_FK, select_with_virtuals, virtual_aggregated_field
from plugin_grand_helpers import save_DAL_log

def test_60_granderp_select_subjects_TODO():
    pass



# @auth.requires_permission('list', 'subject_subject')
def test_62_granderp_subjects():
        from lib.branch import allowed_managers, allowed_subjects_query, is_branch_allowed

        response.subtitle = T('subject_subject__list_form')
        response.files.append(URL('static', 'subject/js/subjects.js'))

        cid = 'subjects'
        filters = [{'label': T('core__all'), 'selected': True, 'data': {}}]
        for c, l in SUBJECT_CLASSES:
            filters.append({'label': l, 'data': {'classes[]': c}})
        db.subject_subject.type.default = None

        # classes = FormField('classes[]', requires=IS_IN_SET(SUBJECT_CLASSES, multiple=True),
        #                label=T('subject_subject__classes'),  target_expression=db.subject_subject.classes)
        classes = FormField(db.subject_subject.classes, name='classes[]', requires=IS_IN_SET(SUBJECT_CLASSES, multiple=True))

        search_fields =  [
                [db.subject_subject.title,
                 Field('manager_ids[]', label=T('subject_subject__manager'),
                       requires=IS_IN_SET(allowed_managers(db, auth), multiple=True))],

                [classes,
                 db.subject_subject.rating_id],

                [db.subject_subject.type, db.address_address.country_id],
                [db.subject_subject.branch_id, db.address_address.city],
                [db.subject_subject.campaign_id, Field('street', label=T('subject_subject__address'))],
                [db.subject_subject.activity_id, db.contact_contact.phone],
            ]



        f = db.subject_subject.contact_id   # virtual field with required_expressions
        f.tablename = f._tablename = "subject_subject"
        f.required_expressions = [db.subject_subject.id]

        cols = [
            db.subject_subject.title,
            db.subject_subject.type,
            db.subject_subject.classes,
            db.subject_subject.contact_id, # Virtual
            # db.subject_subject.address_id, # Virtual


            db.subject_subject.activity_id,
            # Field('subject_subject__main_note', label=T('subject_subject__main_note')),
            # db.address_address.country_id,
            db.address_country.title,
            # db.subject_subject.website,
            db.subject_subject.pay_term,
            # db.subject_subject.credit_bank_id,
        ]

        register = GrandRegister(cols,
                                 cid=cid,
                                 maintable_name='subject_subject',

                                 dalview_left_join_chains=[
                                     [db.subject_subject, db.subject_address, db.address_address, db.address_country]
                                 ],
                                 search_fields = search_fields,
                                 search_fast_filters=filters # fast filters
                                 # dalview_translator=gt

                                 # , crud_urls = {'add': URL('subject', 'add_subject'),
                                 #               #'edit': URL('subject', 'edit_subject')
                                 #              }
                                 # , crud_controller = None # 'subject' # or None for postback

                                 , crud_controller =  'subject' # or None for postback with default SQLFORM() behaviour

                                ,search_formstyle =  None #'divs' if IS_MOBILE else None,
                                # _class = 'mobile_sqlform' if IS_MOBILE else None,
                                 )
        return register.render()
        # form = search_form(
        #     cid,
        #     *search_fields,
        #     # hidden={'subjects_autocomplete_title': URL('subject', 'autocomplete_subject_title.json'),
        #     #         'subjects_autocomplete_phone': URL('subject', 'autocomplete_subject_phone.json'),
        #     #         'subjects_autocomplete_street': URL('subject', 'autocomplete_subject_street.json'),
        #     #         'subjects_autocomplete_city': URL('subject', 'autocomplete_subject_city.json')},
        #     table_name='subject_subject',
        #     # formstyle='divs' if IS_MOBILE else None,
        #     filters=filters
        # )
        #
        # return {'cid': cid, 'form': form}


try:
    gt = GrandTranslator(
        fields=[ db[table].title    for table in 'good  good_group  good_category  good_collection'.split() ],
        language_id=2
    )
except: # ignore in other context
    pass

def test_63a_good_group_Translation():

    target = db.good_group.title
    return UL( DalView(target, translator=gt).execute().column(target) )



def test_63b_represent_FK_virtual():
    fk_field = db.good.group_id
    # fk_field.represent = None
    vf = represent_FK(fk_field)

    # rows = select_with_virtuals( vf ) # TODO : test some renderign...
    rows = select_with_virtuals( db.good.id,  fk_field, vf , db.good.category_id )

    # force_refs_represent_ordinary_int(rows)

    save_DAL_log()
    # return rows
    return dict(rows=rows, dbg=response.toolbar())

    # rows.render()
    # rows = [r for r in rows]
    # rows = rows.as_list()
    # return TABLE(rows)

def test_63c_granderp_widget_autocomplete_multiple():

    # gt = None

    search_fields = [
        # FormField(db.good.group_id, multiple=True, override_widget=True), # override_widget=True  is for SearchSQLFORM
        SearchField(db.good.group_id, multiple=True , override_validator=False),  # override_widget=False  doesn't take effect
        SearchField(db.good.measurement_id, multiple=True , override_validator=True),  # needed if no translation involved
        FormField( db.good.category_id,  comparison='belongs'), # override_widget=True  is for SearchSQLFORM
        # SearchField( db.good.measurement_id, comparison='belongs', override_widget=True ), # override_widget=True  is for SearchSQLFORM
        # SearchField( db.good.measurement_id, comparison='belongs'),
        # db.good.type,
        # db.good.title,
    ]

    # search_form = SearchSQLFORM(*search_fields, translator=gt)
    search_form = GrandSQLFORM(*search_fields, translator=gt)
    return dict( form = search_form )

def test_63d_granderp_good_goods_representFK_multiple():

    # gt = None
    cid = 'goods'

    search_fields = [
        # [db.good.type, db.good.title,  db.good.sku],
        [
            SearchField( db.good.group_id,  multiple=True, override_widget=True) # , override_widget=False?
            # SearchField( db.good.category_id, comparison='belongs'),
        ],
    ]

    db.good.category_id.represent = None
    cols=[
        represent_PK( db.good.id ),
        represent_FK( db.good.group_id ),
        db.good.group_id,
        # db.good.type,
        db.good.sku,
        db.good.title,
        # db.good.category_id, # todo: test
    ]

    register = GrandRegister(cols,
                             # columns_force_FK_table_represent=True,
                             cid=cid,
                             # maintable_name='good',
                             search_fields=search_fields,

                             grid_w2ui_sort =  [ {'field': "sku", 'direction': "asc"} ]
                             , dalview_translator=gt #GrandTranslator( fields = [db.good.title], language_id=2 )
                             # , crud_controller='good'  # or None for postback with default SQLFORM() behaviour
                             , search_formstyle=None  # 'divs' if IS_MOBILE else None,
                             )
    return register.render()





def test_63e_granderp_good_goods_T_Autocomplete_WITH_common_filter():

    # gt = None

    cid = 'goods'

    group_ids_validator = None

    # group_ids_validator=IS_IN_SET(
    #                         translated_set(db, auth, 'good_group', query=(db.good_group.active == True)),
    #                         multiple=True)

    db.good_group._common_filter = lambda q:  db.good_group.active == True


    group_ids_validator=T_IS_IN_DB( gt,
                                    # db(db.good_group.active == True),
                                    db,
                                    db.good_group.id, db.good_group._format,  multiple=True)

    group_ids = SearchField('group_ids', label=db.good.group_id.label,
                    requires=group_ids_validator,

                    target_expression = db.good.group_id,
                    comparison = 'belongs',
                    name_extension = ''
    )
    # group_ids.requires=group_ids_validator
    # group_ids  = SearchField(group_ids, target_expression=db.good.group_id)


    search_fields = [
        [db.good.type, db.good.title],

        # todo
        [ group_ids,
         # db.good.group_id,

         db.good.sku],
        [Field('category_ids[]', label=db.good.category_id.label,
               requires=IS_IN_SET(translated_set(db, auth, 'good_category', query=(db.good_category.active == True)),
                                  multiple=True)),

         Field('produced', label=db.good.produced.label, requires=IS_IN_SET(
             [('T', T('core__yes')), ('F', T('core__no'))]))],
        [Field('collection_ids[]', label=T('good_group__collections'),
               requires=IS_IN_SET(
                   translated_set(db, auth, 'good_collection', query=(db.good_collection.active == True)),
                   multiple=True))]
    ]

        # hidden={'goods_autocomplete_title': URL('good', 'autocomplete_good_titles.json'),
        #         'goods_autocomplete_sku': URL('good', 'autocomplete_good_skus.json')},
        # maintable_name='good'

    # return {'cid': cid, 'form': form, 'row_buttons': None, 'dataFile': db(db.good_settings).select().first().data_file}
    cols=[
        represent_PK( db.good.id, label="Prekė"),
        db.good.category_id,
        # represent_FK( db.good.group_id ),
        db.good.group_id,  # by default will apply represent_FT
        db.good.sku,
        db.good.title,
        db.good.measurement_id,
        db.good.cost, # render: 'number:2'
    ]
    db.good.cost.w2ui = dict(render='number:2')

    # translator =

    register = GrandRegister(cols,

                             columns_force_FK_table_represent=True,
                             cid=cid,
                             maintable_name='good',

                             search_fields=search_fields,

                             w2ui_sort =  [ {'field': "sku", 'direction': "asc"} ]

                             # filters=filters  # fast filters
                             # ,
                             , dalview_translator=gt #GrandTranslator( fields = [db.good.title], language_id=2 )

                             , crud_controller='good'  # or None for postback with default SQLFORM() behaviour

                             , search_formstyle=None  # 'divs' if IS_MOBILE else None,
                             # _class = 'mobile_sqlform' if IS_MOBILE else None,

                             # ,w2grid_options_extra_toolbar_more = "BLA"
                             )
    result = register.render()
    # save_DAL_log()
    return result

def test_63zz_good_goods_NG():

    db.good_group._common_filter = lambda q:  db.good_group.active == True
    db.good_category._common_filter = lambda q:  db.good_category.active == True
    db.good_collection._common_filter = lambda q:  db.good_collection.active == True

    search_fields = [
        [db.good.type,
         SearchField(db.good.title, override_widget=True)],

        [SearchField(db.good.group_id, multiple=True),
         SearchField(db.good.sku, override_widget=True) ],

        [SearchField(db.good.category_id, multiple=True),
         SearchField(db.good.produced, widget=SQLFORM.widgets.options.widget, requires=IS_IN_SET( [('T', T('core__yes')), ('F', T('core__no'))]))
         ],

        [SearchField(db.good_collection_group.collection_id,  label=T('good_group__collections'), multiple=True) ]
        # [SearchField(db.good_collection.id,  label=T('good_group__collections'), multiple=True) ]
    ]

    cols=[
        db.good.category_id,
        db.good.group_id,  # by default will apply represent_FT
        db.good.sku,
        db.good.title,
        db.good.measurement_id,
        db.good.cost, # render: 'number:2'
    ]
    db.good.cost.w2ui = dict(render='number:2')


    register = GrandRegister(
                             cols,
                             columns_force_FK_table_represent=True,
                             cid='goods',
                             # maintable_name='good',
                             search_fields=search_fields,

                             w2ui_sort =  [ {'field': "sku", 'direction': "asc"} ]

                             , dalview_translator=gt #GrandTranslator( fields = [db.good.title], language_id=2 )
                             , crud_controller='good'  # or None for postback with default SQLFORM() behaviour

                             ,dalview_left_join_chain = [db.good_group, db.good_collection_group]
                             , dalview_append_join_chains=True

                             , dalview_smart_groupby=True
                            # , dalview_groupby=db.good.id | title_field
                            #                   | db.good_category.id | category_field |
                            #                   db.good_group.id | group_field |
                            #                 db.measurement.id | measurement_field)

    )
    return register.render()


def test_63z_granderp_good_goods_oldschool_cols():
    from gluon.template import render
    global URL
    content="""
            url: "{{ =URL('good', '{0}_grid.json'.format(cid), user_signature=True) }}",
            columns: [
                {field: "category", caption: "{{ =db.good.category_id.label }}", size: "100%", sortable: true,
                 resizable: true},
                {field: "group", caption: "{{ =db.good.group_id.label }}", size: "100%", sortable: true,
                 resizable: true},
                {field: "sku", caption: "{{ =db.good.sku.label }}", size: "100%", sortable: true, resizable: true},
                {field: "title", caption: "{{ =db.good.title.label }}", size: "100%", sortable: true, resizable: true},
                {field: "measurement", caption: "{{ =db.good.measurement_id.label }}", size: "100%", sortable: true,
                 resizable: true},
                {field: "cost", caption: "{{ =db.good.cost.label }}", size: "100%", sortable: true, resizable: true,
                 render: 'number:2'}
            ],
            sortData: [
                {field: "sku", direction: "asc"}
            ], """

    cid = "goods"
    db = current.db # for context
    print 'dbg locals()', locals()
    context = locals()
    context['URL']=URL
    w2ui_gridoptions_oldschool_js = render(content=content, context=context )

    register = GrandRegister(None,

                             force_FK_table_represent=True,
                             cid=cid,
                             maintable_name='good',
                             search_fields=[[
                                 SearchField( db.good.title, name='title', name_extension='', prepend_tablename=False, )
                             ]],
                             w2ui_gridoptions_oldschool_js = w2ui_gridoptions_oldschool_js

                             # w2ui_sort =  [ {'field': "sku", 'direction': "asc"} ]

                             # filters=filters  # fast filters
                             # ,
                             , dalview_translator=gt #GrandTranslator( fields = [db.good.title], language_id=2 )

                             , crud_controller='good'  # or None for postback with default SQLFORM() behaviour
                             , grid_function='goods_grid'
                             , search_formstyle=None  # 'divs' if IS_MOBILE else None,
                             # _class = 'mobile_sqlform' if IS_MOBILE else None,
                             , recid_4oldschool = db.good.id,
                             # ,w2grid_options_extra_toolbar_more = "BLA"
                             )
    result = register.render()
    # save_DAL_log()
    return result



def test_65_invoice_invoices_searchForm_datesPicking_TODO():

        response.subtitle = T('invoice__list_form')

        cid = 'invoices'

        db.invoice.financial_status.default = None
        db.invoice.type.default = None


        # prepare date range inputs
        use_date_period = Field('date_period', 'boolean',  label=db.invoice.date.label, default=True)
        date_from = Field('date_from', 'date', label=T('core__from'),
                   default=datetime.date.today() - datetime.timedelta(days=30))
        date_untill = Field('date_until', 'date', label=T('core__until'), default=datetime.date.today())

        # use_date_period = SearchField( use_date_period, name="bla", name_extension = '', prepend_tablename = False )
        use_date_period.no_rename = True
        date_from.comparison = '>='
        date_from.no_rename = True
        date_untill.comparison = '<='
        date_untill.no_rename = True

        date_period = [
            [use_date_period],
            [date_from,    date_untill]
        ]

        search_fields = [
            [db.invoice.type],
            [db.invoice.subject_id ,
             # Field('statuses[]', label=db.invoice.status.label, requires=IS_IN_SET(INVOICE_STATUSES, multiple=True))
             SearchField(db.invoice.status, requires=IS_IN_SET(INVOICE_STATUSES, multiple=True), override_validator=False)
             ],
            [db.invoice.number, db.invoice.financial_status],

            # *date_period
            # include date range inputs in form
            [use_date_period],
            [date_from, date_untill]
        ]




        import itertools
        search_fields = itertools.chain(*search_fields) # flatten for SQLFORM.factory
        form =  GrandSQLFORM(*search_fields , table_name="invoice")

        # form =  GrandSQLFORM(*search_fields, form_factory = SOLIDFORM.factory )  # for SOLIDFORM



        form.custom.widget.date_period.parent.append(CAT(
            A(T('calendar__this_week'), _href='#', _style='margin-left: 10px;',
              _onclick=(
                  'setCurrentWeek("#invoice_date_from", "#invoice_date_until", "#invoice_date_period"); return false;')),
            A(T('calendar__this_month'), _href='#', _style='margin-left: 10px;',
              _onclick=(
                  'setCurrentMonth("#invoice_date_from", "#invoice_date_until", "#invoice_date_period"); return false;')),
            A(T('calendar__previous_week'), _href='#', _style='margin-left: 10px;',
              _onclick=(
                  'setPreviousWeek("#invoice_date_from", "#invoice_date_until", "#invoice_date_period"); return false;')),
            A(T('calendar__previous_month'), _href='#', _style='margin-left: 10px;',
              _onclick=(
                  'setPreviousMonth("#invoice_date_from", "#invoice_date_until", "#invoice_date_period"); return false;'))
        ))

        # return form
        return dict( form=form,
                     data=form.vars_as_Row()

                     )


def test_66a_warehouse_batches_SearchForm_with_T_AutocompleteWidget():
    from plugin_GrandTranslator import T_AutocompleteWidget
    cid = 'batches'

    db.warehouse_batch.good_id.widget = T_AutocompleteWidget( gt, request, db.good.title , id_field=db.good.id) # | db.category

    # AUTOCOMPLETE(
    #     db, auth, 'good', url=URL('warehouse', 'search_batch_goods'),
    #     context='form[name=batches__form]'
    # ).widget
    # db.warehouse_batch.supplier_id.widget = AUTOCOMPLETE(
    #     db, auth, 'subject_subject', url=URL('warehouse', 'search_batch_suppliers'),
    #     context='form[name=batches__form]'
    # ).widget

    search_fields = [
        [db.good.group_id,
         SearchField(db.warehouse_batch.good_id) # , override_widget=True
         ],
        [db.good.category_id, db.warehouse_batch.supplier_id],
    ]

    if request.vars._66_inside_Register:
        return search_fields

    import itertools
    search_fields = itertools.chain(*search_fields)  # flatten

    form = GrandSQLFORM(

        *search_fields,
        cid=cid,
        maintable_name='batches'
    )

    return  form
    # return dict(form=form)


def test_66b_aggregate_warehouse_batches_Grid():

    gt = GrandTranslator(fields=[db.good.title], language_id=2) # helpers.get_fields_from_table_format(db.good)

    cid = request.vars.cid
    status = 'success'
    cmd = request.vars.cmd


    from lib.branch import allowed_warehouse_ids, allowed_warehouses_query
    warehouse_ids = allowed_warehouse_ids(db, auth, active=None, as_list=True) or [0]
    query = db.warehouse_batch.warehouse_id.belongs(warehouse_ids) & db.warehouse_batch.good_id > 0

    from lib.currency import convert, get_symbol
    from decimal import Decimal as D

    def _total_field(row):
        # data = db(query & (db.warehouse_batch.good_id == row.good.id)).select(
        data = db((db.warehouse_batch.good_id == row.good.id)).select(
            db.warehouse_batch.ALL,
            join=[db.good.on(db.good.id == db.warehouse_batch.good_id)]
        )

        total = D('0.00000')
        for d in data:
            total += convert(
                db, d.price * d.residual, precision=5, source_currency_id=d.currency_id, rate_date=d.rate_date)
        return total

    total_field_v = Field.Virtual( 'total_field_v', f=_total_field, table_name='good' )
    # total_field_v.required_expressions = [db.warehouse_batch.good_id]

    total_field_vagg = virtual_aggregated_field( 'total_field_vagg',
        # query=query,
        groupby=db.warehouse_batch.good_id,  # expression used to group stuff (also will be column in select)
        # required_expressions=[db.warehouse_batch.ALL],  # cols in select
        required_expressions=[db.warehouse_batch.price, db.warehouse_batch.residual, db.warehouse_batch.currency_id, db.warehouse_batch.rate_date  ],  # cols in select
        f_agg = lambda r, group:  sum(group) or D('0.00000')  ,  # aggregation lambda

        f_group_item = lambda d: convert(db, d.price * d.residual, precision=5, source_currency_id=d.currency_id, rate_date=d.rate_date),  # function applied to group item/row -- like f for ordinary Field.Virtual
        table_name = 'warehouse_batch'
        #, translator = None
        # , left #** select__kwargs
        # , limitby=(0,2)
    )

    columns = [
        # db.good.id,
        represent_PK( db.good.id ) # virtual Expression
        , db.good.title
        # , db.warehouse_batch.id  # for dbg purposes
        #
        ,db.warehouse_batch.received.sum()
        ,db.warehouse_batch.reserved.sum()
        ,db.warehouse_batch.used.sum()
        ,db.warehouse_batch.residual.sum()

        # , total_field_v # ordinary virtual
        , total_field_vagg # virtual aggregate
    ]

    if request.vars._66_inside_Register:
        return columns

    rows = select_with_virtuals(
        *columns
        , translator = gt
        , left = [ db.warehouse_batch.on(db.warehouse_batch.good_id==db.good.id) ]
        # , left = build_joins_chain(db.good, db.warehouse_batch)
        # , groupby =  db.warehouse_batch.good_id #  should be figured out automatically
        # , distinct = db.good.id
        # , orderby = db.good.id|db.good.sku
        , limitby = (0,10)
    )

    if hasattr(current, 'DBG') and current.DBG:
        # current.session.sql_log = PRE(rows.sql_log)
        # current.session.sql_log_nontranslated = PRE(rows.sql_nontranslated_log)
        # save_DAL_log()
        pass

    # return dict(rows=rows)

    return DIV( rows, PRE(rows.sql_log) )

def test_66c_aggregate_warehouse_batches_Register():
    request.vars._66_inside_Register = True
    cols = test_66b_aggregate_warehouse_batches_Grid()
    search_fields = test_66a_warehouse_batches_SearchForm_with_T_AutocompleteWidget()

    register = GrandRegister(cols,

                             columns_force_FK_table_represent=True,
                             cid='batches',
                             # maintable_name='batches',
                             # maintable_name='warehouse_batch',
                             grid_data_name='batches',
                             search_fields=search_fields

                             #, w2ui_sort =  [ {'field': "sku", 'direction': "asc"} ]
                             , dalview_left=[db.warehouse_batch.on(db.warehouse_batch.good_id == db.good.id)]

                             # filters=filters  # fast filters
                             # ,
                             # , translator=gt   # GrandTranslator( fields = [db.good.title], language_id=2 )

                             , crud_controller='warehouse'  # or None for postback with default SQLFORM() behaviour

                             , search_formstyle=None  # 'divs' if IS_MOBILE else None,
                             # _class = 'mobile_sqlform' if IS_MOBILE else None,

                             # ,w2grid_options_extra_toolbar_more = "BLA"
                             )
    return register.render()


controller_dir = dir()
def index():
    from plugin_grand_helpers import make_menu
    make_menu(controller_dir)
    return dict(menu=MENU(response.menu), dbg=response.toolbar() )
