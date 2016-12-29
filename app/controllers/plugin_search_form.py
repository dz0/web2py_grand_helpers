# -*- coding: utf-8 -*-
from plugin_search_form.search_form import SearchField, SearchForm

from plugin_joins_builder.joins_builder import build_joins_chain  # uses another grand plugin

"""
TEST SEARCH FILTERS QUERY from FORM
"""

def tester(search, selected_fields, **kwargs):
    if search.having: 
        kwargs['having'] = search.having  # for aggregates
     
    main_table = selected_fields[0].table
    # search.query &= (db.auth_user.id < 5)  
    if search.query==True: search.query = main_table.id > 0
        
    sql = db(search.query)._select( *selected_fields, **kwargs )    
    print( "DBG SQL: ", sql )
    
    data = SQLFORM.grid(search.query, fields=selected_fields, **kwargs )  # can toggle
    # data = db(search.query).select( *selected_fields, **kwargs )    
    
    menu4tests()
    return dict( data = data, 
                sql = XML(str(db._lastsql[0]).replace('HAVING', "<br>HAVING").replace('WHERE', "<br>WHERE").replace('AND', "<br>AND").replace('LEFT JOIN', '<BR/>LEFT JOIN ')), 
                search_form=search.form,  
                # extra=response.tool, 
                # query=search.query.as_dict(flat=True)   
                query=XML(str(search.query).replace('AND', "<br>AND"))
                )
    
def test1_simple_fields(): # OK
    search = SearchForm(
        SearchField( db.auth_user.first_name),
        SearchField( db.auth_user.email )
    )
    return tester(  search, 
                    selected_fields=[db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 


def def_demo_table():
    db.define_table('item',
                        Field('unit_price', 'double'),
                        Field('quantity', 'integer'),
                        Field('name'),
                        )
         
    # from gluon.contrib.populate import populate
    # populate(db['item'],5)  ; db.commit()
               
def test_virtual_field(): # ?
    def_demo_table()
    db.item.total_price =  Field.Virtual('total_price', lambda row: row.item.unit_price * row.item.quantity)

    grid = SQLFORM.grid(db.item, fields=[db.item.unit_price, db.item.total_price, db.item.quantity ] ) 
    
    return dict(grid=grid)
    
def test9_simple_with_virtual_fields(): # ?

    def_demo_table()
    
    search = SearchForm(
        SearchField( db.item.name),
        SearchField( db.item.quantity, '<' )
    )
    

    db.item.total_price =  Field.Virtual('total_price', lambda row: row.item.unit_price * row.item.quantity)
    
    selected_fields=[db.item.name, db.item.quantity, db.item.total_price ] 
    
    # extra fields,  needed to calculate  "db.item.total_price"
    extra_fields_4virtual = [ db.item.unit_price ]  #
    for f in set(extra_fields_4virtual)-set(selected_fields): 
        f.readable = False
    selected_fields += extra_fields_4virtual
    
    
    # http://web2py.com/books/default/chapter/29/06/the-database-abstraction-layer?search=virtual#New-style-virtual-fields
    return tester(  search, 
                    
                    selected_fields=selected_fields
                    # selected_fields=[db.item.ALL ]  # doesn't work for grid
                 ) 

def test2_same_fields_twice(): # OK
    search = SearchForm(
        SearchField( db.auth_user.first_name, '=' ),
        SearchField( db.auth_user.first_name, 'contains')
    )
    return tester(  search, 
                    selected_fields=[db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 


def test3_fields_from_different_tables(): # OK
    
    search = SearchForm(
        SearchField( db.auth_user.first_name ),
        SearchField( db.auth_user.email ),
        SearchField( db.auth_group.role ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_group.role ] ,
                    left = build_joins_chain( db.auth_user, db.auth_membership, db.auth_group ),
                 )     

    
    
def test4_expr_custom_fields(): # OK
    search = SearchForm(
        # SearchField( db.auth_user.first_name, 'contains' ),
        SearchField( Field( "first_name__custom"),  target_expression=db.auth_user.first_name  ),
        SearchField( Field( "first_name__custom2"), '<',  target_expression=db.auth_user.first_name  ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 

def test5_expr_combination_of_fields(): # OK...
    # from pydal.objects import Expression
    search = SearchForm(
        # SearchField( db.auth_user.first_name, 'contains' ),
        SearchField( Field( "first_name_with_email"),  target_expression=db.auth_user.first_name + db.auth_user.email ),  #  can cause problems if Null 
        # SearchField( Field( "first_name_with_email"),  
                    # target_expression=Expression(db, db._adapter.CONCAT, db.auth_user.first_name, db.auth_user.email) 
                    # ),
        SearchField( db.auth_user.email ),
    )
    
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 




def test6_reference_field_widget(): # OK
    
    search = SearchForm(
        SearchField( db.auth_membership.user_id ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_group.role ] ,
                    left = build_joins_chain( db.auth_user, db.auth_membership, db.auth_group ),
                 )     

def test7_reference_by_anonymous_field(): # OK
    
    search = SearchForm(
        # SearchField( db.auth_membership.user_id ), -- would require left join...
        SearchField( Field('user', 'integer', 
                     requires=IS_IN_DB(db, 'auth_user.id',  db.auth_user._format)), 
                     target_expression = db.auth_user.id
                    ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name] ,
                 )     

def test8_aggregate(): # OK;  BUT: PROBLEM with grid  TODO: automatically detect if target_is_aggregate
    
    search = SearchForm(
        SearchField( Field( "count_user_groups", 'integer'), '>', target_expression=db.auth_group.id.count(), target_is_aggregate=True ),
        SearchField( db.auth_user.first_name ),
        # SearchField( db.auth_group.role )
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_group.id.count() ] ,
                    left = build_joins_chain( db.auth_user, db.auth_membership, db.auth_group ),
                    groupby=db.auth_user.first_name , 
                 )     

def testgrand_SOLIDFORM(): # OK
    # based on def test3_fields_from_different_tables(): 
    from applications.app.modules.solidform import SOLIDFORM
    
    search = SearchForm(
        [ SearchField( db.auth_user.first_name ),       SearchField( db.auth_user.email ) ],
        [ SearchField( db.auth_group.role ), ],
        form_factory = SOLIDFORM.factory,
        formstyle='table3cols'
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_group.role ] ,
                    left = build_joins_chain( db.auth_user, db.auth_membership, db.auth_group ),
                 )     

def testgrand_search_form_4grid():  # TODO
    # based on def test3_fields_from_different_tables(): 
    from applications.app.modules.searching import search_form as grand_search_form
    
    def my_grand_search_form(*fields, **kwargs):
        return grand_search_form('testgrand_search_form_4grid', *fields, **kwargs)
            

    search = SearchForm(
        [ SearchField( db.auth_user.first_name ),       SearchField( db.auth_user.email ) ],
        [ SearchField( db.auth_group.role ), ],
        formstyle='table3cols',
        form_factory = my_grand_search_form
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_group.role ] ,
                    left = build_joins_chain( db.auth_user, db.auth_membership, db.auth_group ),
                 )     


def testgrand_technology_with_good():
    # db.technology.sku.name = "bla.bla"  # IGNORUOJA laukus, su tašku pavadinime

    search = SearchForm(
        SearchField( db.technology.active ),
        SearchField( db.technology.sku, '==' ),
        SearchField( db.technology.title, 'contains' ),
        SearchField( db.technology.type ),
        SearchField( db.technology.good_id ),
    )
    
    menu4tests()
    return dict(  
         searchform = search.form, 
         data_grid = ( SQLFORM.grid(search.query, 
                        fields=[db.technology.sku, db.good.title,
                        # tarp kitko:
                        # db.technology.good_id,  ERROR
                        #   /sqlhtml.py", line 2689, in grid
                        #    nvalue = field.represent(value, row)
                        # TypeError: <lambda>() takes exactly 1 argument (2 given)
                        ], 
                        left=[ db.good.on(db.technology.good_id==db.good.id)], 
                        user_signature=False) 
                        if search.query else None),
         data_rows = db(search.query).select() if search.query else None,
         extra=response.toolbar(), 
         query=XML(str(search.query).replace('AND', "<br>AND"))
    ) 
    
    
def menu4tests():
    test_functions = [x for x in controller_dir if x.startswith('test') and x!='tester' ]    
    response.menu = [('TESTS', False, '', 
                        [  
                            (f, f==request.function, URL(f) )
                            for f in test_functions
                        ]
                    )]
    return response.menu
    
controller_dir = dir()
# menu4tests()        

def index():  
    menu4tests()
    return dict(menu=MENU(response.menu))
    


