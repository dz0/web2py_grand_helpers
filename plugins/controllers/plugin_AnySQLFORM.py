# -*- coding: utf-8 -*-

from pydal.objects import Field #, Row, Expression

from plugin_AnySQLFORM.AnySQLFORM import AnySQLFORM, FormField, get_expressions_from_formfields
from plugin_AnySQLFORM.AnySQLFORM import QuerySQLFORM, SearchField
from plugin_AnySQLFORM.GrandRegister import GrandRegister, DalView
from plugin_AnySQLFORM.GrandRegister import GrandTranslator, T_IS_IN_DB, GrandSQLFORM


# test fields
from  plugin_joins_builder.joins_builder import build_joins_chain


def test_fields():
    orphan_with_target = Field('orphan_name')
    orphan_with_target.target_expression = db.auth_user.last_name + "bla"

    return [

        db.auth_user.first_name,
        db.auth_user.email,
        
        db.auth_user.id,
        db.auth_group.role, # db.auth_group.description,
        
        FormField(db.auth_permission.table_name, requires = IS_IN_DB(db, db.auth_permission.table_name, multiple=True), comparison='equal'),
        SearchField(db.auth_permission.id, requires = IS_IN_DB(db, db.auth_permission, "%(name)s: %(table_name)s %(record_id)s")),
        
        # no_table items   
        Field('user_id', type='reference auth_user'), 
#        FormField('user_id', type='reference auth_user'), 
        
        Field( 'bla'),
#        FormField( 'bla'),
#         FormField( Field( 'expr_as_value' ), target_expression='string_value' ),  # orphan field with expression in kwargs
#         FormField( 'direct_name', target_expression='direct_name_expr' ),  #  name with expression with expression in kwargs
#         FormField( 5, name='str_expr_firstarg' ),  #  expression first -- even if it is just value
        orphan_with_target,
        # expression (as target)
        FormField( db.auth_user.first_name + db.auth_user.last_name, name='full_name', comparison='equals'),
        FormField( Field( 'pure_inputname_in_form'), name_extension='', prepend_tablename=False, target_expression='pure' ),  
    ]
    
def test_queryform():
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
    


def test_anyform():
    
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


def test_dalview_search():

    fields = test_fields()
    fields[0].comparison = 'equals'
    
    form = QuerySQLFORM( *fields )

    form.check_duplicate_fields_by_attrs('target_expression')
    filter = form.build_queries()
    query_data = form.vars_as_Row()

    cols = get_expressions_from_formfields(fields)
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

def test_grandregister_form_and_ajax_records(  ):
    search_fields = test_fields()
    cols = get_expressions_from_formfields(search_fields )

    register = GrandRegister(cols,
                             cid = 'w2ui_test', # w2ui
                             table_name = 'test_grand',
                             search_fields = [search_fields ],
                             left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                             # , response_view = None
                             , translator = gt
                             )

    # response.view = ...
    if request.vars._grid:
        response.view = "generic.json"

        rows = register.w2ui_grid_records()

        # return BEAUTIFY(  [ filter, rows ]  )  # for testing

        if request.vars._grid_dbg:
            def as_htmltable(rows, colnames):
                from gluon.html import TABLE
                return TABLE([colnames] + [[row[col] for col in colnames] for row in rows])
            rows = as_htmltable(rows, [FormField(col).name for col in register.columns]) # for testing

        # from gluon.serializers import json
        # return json(dict(status='success', records = rows ))
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

        ajax_link = A('ajax load records', _href=ajax_url)
        ajax_result_target = DIV( BEAUTIFY(register.records_w2ui() ), _id='grid_records')
        # register.search_form.      add_button( 'ajax load records', ajax_url )
        # result['ajax_records']=
        # result['ats']=ajax_result_target

        # register.search_form[0].insert(0, ajax_link)
        result['form'] = CAT(ajax_result_target , ajax_link, result['form'] )


        return result

def test_grandregister_render(  ):
    search_fields = test_fields() #[5:6]
    search_fields[0].comparison = 'equals'
    cols = get_expressions_from_formfields(search_fields )

    register = GrandRegister(cols,
                             cid = 'w2ui_test', # w2ui
                             table_name = 'test_grand',
                             search_fields = search_fields,
                             # search_fields = [search_fields ],  # for SOLIDFORM?
                             left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                             # , response_view = None
                             , translator=gt
                             )
    register.render()


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


def test_grandtranslator_expressions():

    tests = [
        db.auth_user.first_name,  # Field
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

def test_grandtranslator_dalview():

    expr = db.auth_user.first_name + db.auth_user.last_name

    selection = DalView( expr ,  query=db.auth_user,
                         left_join_chains=[[db.auth_user, db.auth_membership, db.auth_group]],
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



def test_grandtranslator_dalview_search():

    fields = test_fields()

    column_fields= fields[:4]   # include expression

    expr_col = fields[-2]
    expr_col.comparison = 'equals' # # We will test Expression with IS_IN_SET widget
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
    return dict(menu=MENU(response.menu))