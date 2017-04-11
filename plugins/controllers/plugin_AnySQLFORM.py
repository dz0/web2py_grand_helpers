# -*- coding: utf-8 -*-

from pydal.objects import Field #, Row, Expression

from plugin_AnySQLFORM.AnySQLFORM import AnySQLFORM, FormField, get_expressions_from_formfields
from plugin_AnySQLFORM.AnySQLFORM import QuerySQLFORM, SearchField
from plugin_AnySQLFORM.GrandRegister import GrandRegister, grand_select
from plugin_AnySQLFORM.GrandRegister import GrandTranslator, T_IS_IN_DB, GrandSQLFORM
from plugin_AnySQLFORM.DalView import DalView

from plugin_AnySQLFORM.helpers import save_DAL_log

# test fields
from  plugin_joins_builder.joins_builder import build_joins_chain

from applications._test_app_linked.modules import plugin_AnySQLFORM

orphan_with_target = Field('orphan_name')
orphan_with_target.target_expression = db.auth_user.last_name + "bla"

def test_fields():
    global TEST_FIELDS
    if not 'TEST_FIELDS' in globals():
        TEST_FIELDS = [

            db.auth_user.first_name,
            db.auth_user.email,

            db.auth_user.id,
            db.auth_group.role,  # db.auth_group.description,

            FormField(db.auth_permission.table_name, requires=IS_IN_DB(db, db.auth_permission.table_name, multiple=True),
                      comparison='equal'),
            SearchField(db.auth_permission.id,
                        requires=IS_IN_DB(db, db.auth_permission, "%(name)s: %(table_name)s %(record_id)s")),

            # no_table items
            Field('user_id', type='reference auth_user'),
            #        FormField('user_id', type='reference auth_user'),

            Field('bla'),
            #        FormField( 'bla'),
            #         FormField( Field( 'expr_as_value' ), target_expression='string_value' ),  # orphan field with expression in kwargs
            #         FormField( 'direct_name', target_expression='direct_name_expr' ),  #  name with expression with expression in kwargs
            #         FormField( 5, name='str_expr_firstarg' ),  #  expression first -- even if it is just value
            orphan_with_target,
            # expression (as target)
            FormField(db.auth_user.first_name + db.auth_user.last_name, name='full_name', comparison='equal'),
            FormField(Field('pure_inputname_in_form'), name_extension='', prepend_tablename=False, target_expression='pure'),
        ]
    return TEST_FIELDS

# test_fields()


def test_20_queryform():
    fields = test_fields() 
    
    # db.auth_permission._format = "%(name)s %(table_name)s (%(id)s)"

    form = QuerySQLFORM( *fields )
    query_data = form.vars_as_Row()
    form.check_duplicate_fields_by_attrs('target_expression')
    filter = form.build_queries()
        
    return dict(
            filter = filter,
            form=form, 
            query_data = repr(query_data),
            # data = data,
            vars=form.vars,
            # vars_dict=repr(form.vars),
            )
    

def test_10_anyform():
    
    user_full_name = db.auth_user.first_name + db.auth_user.last_name
    
    # db.auth_permission._format = "%(name)s %(table_name)s (%(id)s)"
    
    fields = test_fields()
    
    form = AnySQLFORM( *fields )
    # form = SQLFORM.factory( *fields )

    data = form.vars_as_Row()
        
    return dict(
            form=form, 
            data_dict = repr(data),
            data = data,
            vars=form.vars,
            vars_dict=repr(form.vars),
            )


def test_22_dalview_search():

    fields = test_fields()
    fields[0].comparison = 'equal'
    
    form = QuerySQLFORM( *fields )

    form.check_duplicate_fields_by_attrs('target_expression')
    filter = form.build_queries()
    query_data = form.vars_as_Row()

    cols = get_expressions_from_formfields(fields, include_orphans=False ) # for 27 should change to True
    print "dbg cols", cols

    selection = DalView(*cols, query=filter.query, having=filter.having,
                  left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                  )

    sql = selection.get_sql()
    print( "DBG SQL: ", sql )

    # data = SQLFORM.grid(search.query, fields=selected_fields, **kwargs )  # can toggle
    data = selection.execute()

    return dict(
            sql = sql,
            filter = filter, # query and having
            form=form, 
            # query_data = repr(query_data),
            data = data,
            vars=form.vars,
            # vars_dict=repr(form.vars),
            )    

test_dalview_search = test_22_dalview_search
def test_27_virtual_field_dalview_search_TODO_orNotTODO():
    vf = db.auth_user.vf = TEST_FIELDS[0] = Field.Virtual( "virtual", label='Virtualus', f=lambda row: "bla", table_name='auth_user')
    vf.table = vf._table = db.auth_user


    return test_dalview_search()



#################################################################################
#####################                      ###########################
#####################    Translator        ###########################
#####################                      ###########################
#################################################################################
gt = GrandTranslator(
    fields = [db.auth_user.first_name,   db.auth_group.role],   # we want to get tranlations only for first_name and role
    language_id=2
)
# def grandform( form_factory=SQLFORM.factory ):







def populate_fake_translations():

    # field = db.auth_user.first_name
    # field = db.auth_group.role

    print 'dbg select', db()._select( 'id', field )
    for r in db().select( 'id', field ):
        db.translation_field.insert(
            tablename = field._tablename,
            fieldname = field.name,
            rid = r['id'],
            language_id = 2,
            value = "EN_"+r[field]
        )


def test_30_grandtranslator_expressions():

    tests = [
        db.auth_user.first_name,  # Field
        db.auth_user.last_name,  # Field - nontranslated
        db.auth_user.first_name + db.auth_user.last_name, # Flat Expression - 1 tranlsation
        db.auth_user.first_name + db.auth_group.role, # Flat Expression - 2 translations
        db.auth_user.first_name.contains('s'),  # Flat Query
        # Alias ?

        # structured / hierarchical / complex cases
        (db.auth_group.role+(db.auth_user.first_name + db.auth_user.last_name)), # complex Expression
        (db.auth_user.first_name.contains('s') | (db.auth_user.first_name=="John") ) & (db.auth_user.last_name=="BLA"),  # complex Query

        # list of expressions (any level depth/structure)
        tuple( [ ('list',  db.auth_group.role), db.auth_user.first_name.contains(['s', 'd']), ])
        ]

    def repr_t(t):  return map(str, [t.expr]+t.left  )

    results =  [ {expr: repr_t( gt.translate( expr ))}  for expr in tests]
    return dict(tests=results)

def test_31_grandtranslator_dalview():

    expr = db.auth_user.first_name + db.auth_user.last_name

    selection = DalView( expr ,  query=db.auth_user,
                         # left_join_chains=[[db.auth_user, db.auth_membership, db.auth_group]],
                         # left = build_joins_chain(db.auth_user, db.auth_membership, db.auth_group),
                         translator = gt
                  )

    sql = selection.get_sql(translate=False)
    sql_translated = selection.get_sql()

    return dict(
        sql=PRE(  sql  .replace("LEFT", "\nLEFT")),
        sql_translated=PRE(sql_translated  .replace("LEFT", "\nLEFT").replace("COALESCE", "\nCOALESCE")),
        data = selection.execute()
    )



def test_32_grandtranslator_dalview_search():

    fields = test_fields()

    column_fields= fields[:4]   # include expression

    expr_col = fields[-2]
    expr_col.comparison = 'equal' # # We will test Expression with IS_IN_SET widget
    search_fields= [ expr_col ] + column_fields

    form = GrandSQLFORM(*search_fields, translator=gt) # uses translator for validators

    # form.check_duplicate_fields_by_attrs('target_expression')
    filter = form.build_queries()
    query_data = form.vars_as_Row()


    # selection = DalView(*tfields, query=tquery, having=thaving, left_given = tleft,
    selection = DalView(*column_fields, query = filter.query, having = filter.having, left_given = (),
                        distinct = True,
                        left_join_chains=[[db.auth_user, db.auth_membership, db.auth_group, db.auth_permission]],
                        translator = gt
                        )

    # cols = get_expressions_from_formfields(fields)
    # print "dbg cols", cols
    # selection.fields = cols

    sql = selection.get_sql(translate=False)
    sql_translated = selection.get_sql()

    print("DBG SQL: ", sql_translated)

    # data = SQLFORM.grid(search.query, fields=selected_fields, **kwargs )  # can toggle
    data = selection.execute()

    return dict(
        sql=PRE(sql.replace("LEFT", "\nLEFT")),
        sql_translated=PRE(sql_translated.replace("LEFT", "\nLEFT").replace("COALESCE", "\nCOALESCE")),
        filter=filter,  # query and having
        form=form,
        # query_data = repr(query_data),
        data=data,
        vars=form.vars,
        # vars_dict=repr(form.vars),
    )

    """    """



#################################################################################
#####################                      ###########################
#####################    GRAND REGISTER        ###########################
#####################                      ###########################
#################################################################################
def test_40_GrandForm():
    fields = test_fields()

    fields[0].comparison = 'equal'
    a, b, c, d = fields[:4]
    fields = [ [a, b], [c, d] ]  # prepare for solidform

    def my_grand_search_form(*fields, **kwargs):
        from searching import search_form as grand_search_form
        return grand_search_form('test', *fields, **kwargs)

    # kwargs.setdefault( 'form_factory', my_grand_search_form )
    form_factory = my_grand_search_form

    form = GrandSQLFORM(*fields,
                        form_factory=form_factory
                        , formstyle =  None #'table3cols' or 'divs' # if not defined -- 'table2cols'
                        )

    data = form.vars_as_Row()

    return dict(form=form)
    return form

def test_41_grandregister_form_and_ajax_records(  ):
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
        rows = register.w2ui_grid_records()

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

        result = register.form()

        # for debug purposes:
        # tablename = register.search_form.table._tablename
        ajax_url = "javascript:ajax('%s', %s, 'grid_records'); " % (
                                                URL(vars=dict(_grid=True, _grid_dbg=True), extension=None)  ,
                                                [f.name for f in register.search_fields]
                   )

        ajax_link = A('ajax load records', _href=ajax_url, _id="ajax_loader_link")
        ajax_result_target = DIV( BEAUTIFY(register.w2ui_grid_records() ), _id='grid_records')
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

    from plugin_AnySQLFORM.GrandRegister import create_fast_filters, SearchField
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
                             table_name = 'test_grand',

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
def test_42_grandregister_with_just_SQLFORM():
    global use_grand_search_form
    use_grand_search_form = False
    return test_grandregister()



def test_60_subjects_country_joins_chain():
    join_expr = build_joins_chain( db.subject_subject, db.subject_address,  db.address_address, db.address_country )
    # print join_expr
    # rows = db().select(db.subject_subject.title,  db.address_country.title, left=join_expr, limitby=(0,30))
    rows = grand_select(db.subject_subject.title,  db.address_country.title, left=join_expr, limitby=(0,30))

    return dict( rows=rows, sql=db._lastsql, dbg=response.toolbar() )
    # join_expr= build_joins_chain( db.subject_subject, db.subject_address, db.address_address, db.address_country)
    # return str(join_expr)

from plugin_AnySQLFORM.DalView import represent_PK, represent_FK, select_with_virtuals, virtual_aggregated_field
from plugin_AnySQLFORM.helpers import save_DAL_log

def test_60_grand_select_subjects():
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
                                 table_name='subject_subject',

                                 left_join_chains=[
                                     [db.subject_subject, db.subject_address, db.address_address, db.address_country]
                                 ],
                                 search_fields = search_fields,
                                 filters=filters # fast filters
                                 # translator=gt

                                 # , crud_urls = {'add': URL('subject', 'add_subject'),
                                 #               #'edit': URL('subject', 'edit_subject')
                                 #              }
                                 # , crud_controller = None # 'subject' # or None for postback

                                 , crud_controller =  'subject' # or None for postback with default SQLFORM() behaviour

                                ,formstyle =  None #'divs' if IS_MOBILE else None,
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

# from  plugin_AnySQLFORM.helpers import get_fields_from_table_format



def test_63b_represent_FK_virtual():
    fk_field = db.good.group_id
    # fk_field.represent = None
    vf = represent_FK(fk_field)

    # rows = grand_select( vf ) # TODO : test some renderign...
    rows = grand_select( db.good.id,  fk_field, vf , db.good.category_id )

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
        # FormField(db.good.group_id, multiple=True, override_validator=True), # override_validator=True  is for QuerySQLFORM
        SearchField(db.good.group_id, multiple=True , override_validator=False),  # override_validator=False  doesn't take effect
        SearchField(db.good.measurement_id, multiple=True , override_validator=True),  # needed if no translation involved
        FormField( db.good.category_id,  comparison='belongs'), # override_validator=True  is for QuerySQLFORM
        # SearchField( db.good.measurement_id, comparison='belongs', override_validator=True ), # override_validator=True  is for QuerySQLFORM
        # SearchField( db.good.measurement_id, comparison='belongs'),
        # db.good.type,
        # db.good.title,
    ]

    # search_form = QuerySQLFORM(*search_fields, translator=gt)
    search_form = GrandSQLFORM(*search_fields, translator=gt)
    return dict( form = search_form )

def test_63d_granderp_good_goods_representFK():

    # gt = None
    cid = 'goods'

    search_fields = [
        # [db.good.type, db.good.title,  db.good.sku],
        [
            SearchField( db.good.group_id,  multiple=True),  # comparison='belongs',
            # SearchField( db.good.category_id, comparison='belongs'),
        ],
    ]

    db.good.category_id.represent = None
    cols=[
        represent_PK( db.good.id ),
        represent_FK( db.good.group_id ),
        # db.good.group_id,
        db.good.type,
        db.good.sku,
        db.good.title,
        db.good.category_id, # todo: test
    ]

    register = GrandRegister(cols,
                             force_FK_table_represent=True,
                             cid=cid,
                             table_name='good',
                             search_fields=search_fields,

                             # force_FK_table_represent = False, # default True

                             w2ui_sort =  [ {'field': "sku", 'direction': "asc"} ]
                             , translator=gt #GrandTranslator( fields = [db.good.title], language_id=2 )
                             , crud_controller='good'  # or None for postback with default SQLFORM() behaviour
                             , formstyle=None  # 'divs' if IS_MOBILE else None,
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

    db = current.db
    cid = "goods"
    print 'dbg locals()', locals()
    context = locals()
    context['URL']=URL
    w2ui_coldata_oldschool_js = render(content=content, context=context )

    register = GrandRegister(None,

                             force_FK_table_represent=True,
                             cid=cid,
                             table_name='good',
                             search_fields=[[
                                 SearchField( db.good.title, name='title', name_extension='', prepend_tablename=False, )
                             ]],
                             grid_w2ui_coldata_oldschool_js = w2ui_coldata_oldschool_js

                             # w2ui_sort =  [ {'field': "sku", 'direction': "asc"} ]

                             # filters=filters  # fast filters
                             # ,
                             , dalview_translator=gt #GrandTranslator( fields = [db.good.title], language_id=2 )

                             , crud_controller='good'  # or None for postback with default SQLFORM() behaviour
                             , grid_function='goods_grid'
                             , formstyle=None  # 'divs' if IS_MOBILE else None,
                             # _class = 'mobile_sqlform' if IS_MOBILE else None,

                             # ,w2grid_options_extra_toolbar_extra = "BLA"
                             )
    result = register.render()
    # save_DAL_log()
    return result


def test_63e_granderp_good_goods():

    # gt = None

    cid = 'goods'

    group_ids_validator = None

    # group_ids_validator=IS_IN_SET(
    #                         translated_set(db, auth, 'good_group', query=(db.good_group.active == True)),
    #                         multiple=True)

    # db.good_group._common_filter = lambda q: q& db.good_group.active == True
    group_ids_validator=T_IS_IN_DB( gt,
                                    db(db.good_group.active == True), db.good_group.id, db.good_group._format,  multiple=True)

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
        # table_name='good'

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

                             force_FK_table_represent=True,
                             cid=cid,
                             table_name='good',
                             search_fields=search_fields,

                             w2ui_sort =  [ {'field': "sku", 'direction': "asc"} ]

                             # filters=filters  # fast filters
                             # ,
                             , dalview_translator=gt #GrandTranslator( fields = [db.good.title], language_id=2 )

                             , crud_controller='good'  # or None for postback with default SQLFORM() behaviour

                             , formstyle=None  # 'divs' if IS_MOBILE else None,
                             # _class = 'mobile_sqlform' if IS_MOBILE else None,

                             # ,w2grid_options_extra_toolbar_extra = "BLA"
                             )
    result = register.render()
    # save_DAL_log()
    return result

def test_70_AnySQLFORM_edit():
    table = db.auth_user
    item = table(2)

    fields = [table[f] for f in table.fields]

    for f in fields:
        # f.default = item[f]   # prepopulating form using "default" value  -- doesn't work with .process()
        f.override_validator = False

    form = AnySQLFORM(*fields)

    # return form  # plain from...

    # prepopulating form using form.vars
    # http://web2py.com/books/default/chapter/29/07/forms-and-validators#Pre-populating-the-form
    for f in form.formfields:
        form.vars[f.name] = item[f.target_expression]

    form.process()
    return dict( form=form, vars=form.vars_as_Row() )

from datetime import datetime
def test_65_invoice_invoices_searchForm_datesPicking_TODO():

        response.subtitle = T('invoice__list_form')

        cid = 'invoices'

        db.invoice.financial_status.default = None
        db.invoice.type.default = None


        search_fields = [
            [db.invoice.type],
            [db.invoice.subject_id,
             # Field('statuses[]', label=db.invoice.status.label, requires=IS_IN_SET(INVOICE_STATUSES, multiple=True))],
             SearchField(db.invoice.status, requires=IS_IN_SET(INVOICE_STATUSES, multiple=True), override_validator=False)
             ],
            [db.invoice.number, db.invoice.financial_status],
            # [Field('date_period', 'boolean', label=db.invoice.date.label, default=True)],
            # [Field('date_from', 'date', label=T('core__from'),
            #        default=datetime.date.today() - datetime.timedelta(days=30)),
            #  Field('date_until', 'date', label=T('core__until'), default=datetime.date.today())],

        ]


        # form.custom.widget.date_period.parent.append(CAT(
        #     A(T('calendar__this_week'), _href='#', _style='margin-left: 10px;',
        #       _onclick=(
        #           'setCurrentWeek("#invoice_date_from", "#invoice_date_until", "#invoice_date_period"); return false;')),
        #     A(T('calendar__this_month'), _href='#', _style='margin-left: 10px;',
        #       _onclick=(
        #           'setCurrentMonth("#invoice_date_from", "#invoice_date_until", "#invoice_date_period"); return false;')),
        #     A(T('calendar__previous_week'), _href='#', _style='margin-left: 10px;',
        #       _onclick=(
        #           'setPreviousWeek("#invoice_date_from", "#invoice_date_until", "#invoice_date_period"); return false;')),
        #     A(T('calendar__previous_month'), _href='#', _style='margin-left: 10px;',
        #       _onclick=(
        #           'setPreviousMonth("#invoice_date_from", "#invoice_date_until", "#invoice_date_period"); return false;'))
        # ))

        import itertools
        search_fields = itertools.chain(*search_fields) # flatten
        form =  GrandSQLFORM(*search_fields )
        # return form
        return dict( form=form )


def test_66a_warehouse_batches_SearchForm_with_T_AutocompleteWidget():
    from plugin_AnySQLFORM.GrandTranslator import T_AutocompleteWidget
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
         SearchField(db.warehouse_batch.good_id) # , override_validator=True
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
        table_name='batches'
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
        # query = db.warehouse_batch.good_id==271,
        groupby=db.warehouse_batch.good_id,  # expression used to group stuff (also will be column in select)
        # required_expressions=[db.warehouse_batch.ALL],  # cols in select
        required_expressions=[db.warehouse_batch.price, db.warehouse_batch.residual, db.warehouse_batch.currency_id, db.warehouse_batch.rate_date  ],  # cols in select
        f_agg = lambda r, group:  sum(group) or D('0.00000')  ,  # aggregation lambda
        # f_agg = lambda r, group:  'bla',  # aggregation lambda
        # f_group_item = lambda d: convert(db, d.price * d.residual, precision=5, source_currency_id=d.currency_id, rate_date=d.rate_date),  # function applied to group item/row -- like f for ordinary Field.Virtual
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

                             force_FK_table_represent=True,
                             cid='batches',
                             table_name='batches',
                             search_fields=search_fields

                             #, w2ui_sort =  [ {'field': "sku", 'direction': "asc"} ]
                             , left=[db.warehouse_batch.on(db.warehouse_batch.good_id == db.good.id)]

                             # filters=filters  # fast filters
                             # ,
                             # , translator=gt   # GrandTranslator( fields = [db.good.title], language_id=2 )

                             , crud_controller='warehouse'  # or None for postback with default SQLFORM() behaviour

                             , formstyle=None  # 'divs' if IS_MOBILE else None,
                             # _class = 'mobile_sqlform' if IS_MOBILE else None,

                             # ,w2grid_options_extra_toolbar_extra = "BLA"
                             )
    return register.render()

def test_70_group_by_val():
    rows = db().select(db.auth_user.first_name, db.auth_group.ALL,
                left=build_joins_chain( db.auth_user, db.auth_membership, db.auth_group )
                )
    key_field = db.auth_user.first_name
    rows_grouped = rows.group_by_value( key_field )
    # names = rows.column( key_field )

    for name in rows_grouped:
        rows_grouped[name] = [row['auth_group']['role'] for row in  rows_grouped[name] ]
        # rows_grouped[name] = BEAUTIFY(  rows_grouped[name]  )
    return CAT( SQLTABLE(rows), TABLE(map( TR, rows_grouped.items()) ), _border=2  )

def test_80_postgre_distinct():
    sel = DalView(db.auth_user.first_name, distinct=True, translator=gt) # orderby=db.auth_user.first_name
    print sel.get_sql()
    return CAT(sel.execute(), PRE(sel.get_sql()) )

def test_00_dev_auth_has_permission():
    return dict(a=auth.has_permission('add', 'auth_user'))

def test_01_virtual_field():
    db.define_table('demo',
                    Field('name')
                    , Field.Virtual('virtual', f=lambda r: 'v...'+r.demo.name, table_name='demo')
                    , Field('bla', default="bla")
                    )
    db.demo.truncate()
    for x in "ABCD":  db.demo.insert( name=x ) # populate

    # return db.demo.fields
    return SQLFORM.grid(db.demo, fields=[db.demo.virtual, db.demo.name])
    # return SQLFORM.grid(db.demo, fields=[db.demo[f] for f in db.demo.fields] +[db.demo.virtual] )
    return db().select(db.demo.ALL)

def init_tables_AB():
    db.define_table('A',  Field('f1'),  Field('f2'),  Field.Virtual('vf3_agg', f=lambda r:None, table_name='A')  )
    db.define_table('B',
                    Field('f1'),
                    Field.Virtual('vf2', f=lambda r: "virtual:"+r.A.f1, table_name='B'),
                    # FieldVirtual_WithDependancies('vf2', f=lambda r: "virtual:"+r.A.f1, table_name='B'),
                    Field('f3'),
                    Field('A_id', db.A)
                    )
    db.B.vf2.required_expressions=[db.A.f1]
    db.B.vf2.required_joins= [ db.A.on(db.B.A_id==db.A.id) ] # build_joins_chain(db.B, db.A)

    # TODO: test:
    # from plugin_AnySQLFORM import virtual_field
    # db.B.vf2 = virtual_field( 'vf2', f=lambda r: "virtual:"+r.A.f1, table_name='B',
    #                               required_expressions=[db.A.f1],
    #                               required_joins= [ db.A.on(db.B.A_id==db.A.id) ]
    # )

    db.A.truncate()
    db.B.truncate()

    for tablename in "AB":
        table = db[tablename]
        for nr in "1234":
            vals = {}
            for fieldname in table.fields[1:]:
                if fieldname.endswith('_id'):
                    vals[fieldname] = 5-int(nr)
                else:
                    vals [fieldname] = "%s%s:%s"% (tablename, fieldname, nr )

            table.insert( **vals )

    # generate more records in B with refs to A
    table = db.B
    import random
    for nr in range(5, 10):
        vals = {}
        for fieldname in table.fields[1:]:
            if fieldname=='A_id':
                vals[fieldname] = random.randint(1, 4)
            else:
                vals[fieldname] = "%s%s:%s" % (table._tablename, fieldname, nr)
        table.insert(**vals)

def test_02_virtual_field():
    init_tables_AB()

    # http://www.web2py.com/books/default/chapter/29/06/the-database-abstraction-layer#New-style-virtual-fields
    db.define_table('item',
                    Field('unit_price', 'double'),
                    Field('quantity', 'integer'))



    db.item.total_price = Field.Virtual('total_price',
                      lambda row: row.item.unit_price * row.item.quantity
                      # lambda row: row.A.f1 * row.item.quantity
                    , table_name='item' )

    db.item.truncate()
    for i in range(4):
        db.item.insert(unit_price=[5, 7, 2, 5][i], quantity=i)

    rows = db().select(db.item.ALL, db.item.quantity*2, join=db.A.on(db.A.id==1) )

    # return rows
    prices = [row.item.total_price for row in rows ]

    return str(prices)





def dbg():
    form = FORM(
        INPUT(_value='clear_session', _name='clear_session',  _type='submit' )
        , INPUT(_value='refresh', _name='refresh',  _type='submit' )
    )
    if request.vars.clear_session:
        session.clear()
        with open('/tmp/web2py_sql.log.html', 'w') as f:
            pass

    if request.vars.refresh:
        redirect(URL())

    try:
        with open('/tmp/web2py_sql.log.html') as f:
            sql_log_full=XML(f.read())
    except:
        sql_log_full=''

    return dict(dbg=response.toolbar(), form=form, session=session, sql_log_full=sql_log_full)


def test_24_virtual_field():
    """
        Example:
        Table A: f1, f2
        Table B: f1, vf2(required_expressions: A.f1), f3, A_id

    columns = [ B.f1, B.vf2,  B.f3*3 ]
    -->
    virtual: [ B.vf2 ]
    selectable: [ B.f1, B.f3*3, A.f1 ]
    nonshown: [ A.f1 ]

    """

    init_tables_AB()
    columns = [db.B.f1, db.B.vf2, db.A.f2+'bla'] # Field, Field.Virtual, Expression

    # testGrid = False
    # if testGrid:
    #     db.A.f1.readable = False
    #     return SQLFORM.grid(db.B, fields=columns[:2]+[db.A.f1], left=db.A.on(db.A.id==db.B.id) )


    return grand_select(*columns) # same as  select_with_virtuals(db, *columns) #.as_json()

    # return select_with_virtuals(db, *columns, left=db.A.on(db.A.id==db.B.id)) #.as_json()
    # return db().select(db.demo.ALL)

def test_25_virtual_field_Represent():

    init_tables_AB()
    columns = [db.B.f1, db.B.vf2, db.A.f2 + 'bla']  # Field, Field.Virtual, Expression

    db.B.vf2.represent = lambda value: STRONG(value) # virtual field
    # need to inject representation in virutal field function directly
    vfunction = db.B.vf2.f  # to prevent recursive definition ;)
    db.B.vf2.f = lambda row: db.B.vf2.represent( vfunction(row)  )

    rows =  grand_select(*columns)

    db.B.f1.represent = lambda value: STRONG(value)  # ordinary field representation can be given even after selection

    rows.render()
    return rows

def test_26_virtual_field_Aggregate__grand_select():
    """
        Example:
        Table A: f1, f2, vf3_agg(Aggregate list A)
        Table B: f1, vf2(required_expressions: A.f1), f3, A_id

    columns = [ B.f1, B.vf2,  B.f3*3 ]
    -->
    virtual: [ B.vf2 ]
    selectable: [ B.f1, B.f3*3, A.f1 ]
    nonshown: [ A.f1 ]

    """

    init_tables_AB()
    gt.fields.append(db.B.f1)  # instruct translator to lookup B.f1

    # db.A.vf3_agg.f = lambda r: "ref %s" % r[db.B.id]

    # db.A.vf3_agg.required_expressions=[db.B.id]      # Postgre could be:, 'array_to_string(array_agg(B.id), ',')']
    # db.A.vf3_agg.required_joins= [ db.B.on(db.B.A_id==db.A.id) ] # build_joins_chain(db.B, db.A)
    db.A.vf3_agg.aggregate = dict(    groupby=db.A.id,
                                      select__kwargs=dict(left=[db.B.on(db.B.A_id == db.A.id)] ),
                                      # required_expressions=[db.B.id, db.B.f1],
                                      required_expressions=[db.B.id, db.B.f1],
                                      # f =   lambda row, group:  ', '.join( map(, map(lambda r: r[db.B.id], group )) )
                                      f =   lambda row, group:  ', '.join(  map(lambda r: "%(id)s %(f1)s"%r[db.B], group )  )
                                      , translator = gt
                                      )




    ########
    # Define cols
    columns = [db.A.id, db.A.vf3_agg ]; left=None # Field, Field.Virtual, Expression
    # columns = [db.A.id, db.B.id, db.A.vf3_agg ]; left = build_joins_chain(db.A, db.B) # Field, Field.Virtual, Expression

    # testGrid = False
    # if testGrid:
    #     db.A.f1.readable = False
    #     return SQLFORM.grid(db.B, fields=columns[:2]+[db.A.f1], left=db.A.on(db.A.id==db.B.id) )

    # rows = select_with_virtuals(
    rows = grand_select(
                    *columns
                    , query = db.A.id > 1
                    # , groupby=db.A.id
                    , translator = gt
                    , left=left
                    , orderby=db.A.id
                    , limitby=(0,5)
                          )

    return dict( rows=rows )


controller_dir = dir()
def menu4tests():
    test_functions = [x for x in controller_dir if x.startswith('test') and x!='tester']
    response.menu = [('TESTS', False, '',
                        [
                            (f, f==request.function, URL(f) )
                            for f in test_functions
                        ]
                     ),
                     ('populate auth tables', False, URL('populate_fake_auth') ),
                    ]
    return response.menu



def index():
    menu4tests()
    return dict(menu=MENU(response.menu), dbg=response.toolbar())