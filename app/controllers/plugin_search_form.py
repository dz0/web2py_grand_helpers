# -*- coding: utf-8 -*-
from plugin_search_form.search_form import queryFilter, SearchForm

from plugin_joins_builder.joins_builder import build_joins_chain  # uses another grand plugin

"""
TEST SEARCH FILTERS QUERY from FORM
"""

def tester(search, selected_fields, **kwargs):
    # data = SQLFORM.grid((db.auth_user.id < 5) & search.query, fields=selected_fields, **kwargs )  # can toggle
    if search.having: 
        kwargs['having'] = search.having  # for aggregates
        
    # sql = db((db.auth_user.id < 5) & search.query)._select( *selected_fields, **kwargs )    
    # print( "DBG SQL: ", sql )
    data = db((db.auth_user.id < 5) & search.query).select( *selected_fields, **kwargs )    
    menu4tests()
    return dict( data = data, 
                sql = db._lastsql, 
                search_form=search.form,  
                # extra=response.tool, 
                # query=search.query.as_dict(flat=True)   
                query=XML(str(search.query).replace('AND', "<br>AND"))
                )
    
def test1_simple_fields(): # OK
    search = SearchForm(
        queryFilter( db.auth_user.first_name),
        queryFilter( db.auth_user.email )
    )
    return tester(  search, 
                    selected_fields=[db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 

def test2_same_fields_twice(): # OK
    search = SearchForm(
        queryFilter( db.auth_user.first_name, '=' ),
        queryFilter( db.auth_user.first_name, 'contains')
    )
    return tester(  search, 
                    selected_fields=[db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 


def test3_fields_from_different_tables(): # OK
    
    search = SearchForm(
        queryFilter( db.auth_user.first_name ),
        queryFilter( db.auth_user.email ),
        queryFilter( db.auth_group.role ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_group.role ] ,
                    left = build_joins_chain([ db.auth_user, db.auth_membership, db.auth_group ]),
                 )     

    
    
def test4_expr_custom_fields(): # OK
    search = SearchForm(
        # queryFilter( db.auth_user.first_name, 'contains' ),
        queryFilter( Field( "first_name__custom"),  target_expression=db.auth_user.first_name  ),
        queryFilter( Field( "first_name__custom2"), '<',  target_expression=db.auth_user.first_name  ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 

def test5_expr_combination_of_fields(): # OK...
    # from pydal.objects import Expression
    search = SearchForm(
        # queryFilter( db.auth_user.first_name, 'contains' ),
        queryFilter( Field( "first_name_with_email"),  target_expression=db.auth_user.first_name + db.auth_user.email ),  #  can cause problems if Null 
        # queryFilter( Field( "first_name_with_email"),  
                    # target_expression=Expression(db, db._adapter.CONCAT, db.auth_user.first_name, db.auth_user.email) 
                    # ),
        queryFilter( db.auth_user.email ),
    )
    
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 




def test6_reference_field_widget(): # OK
    
    search = SearchForm(
        queryFilter( db.auth_membership.user_id ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_group.role ] ,
                    left = build_joins_chain([ db.auth_user, db.auth_membership, db.auth_group ]),
                 )     

def test7_reference_by_anonymous_field(): # OK
    
    search = SearchForm(
        # queryFilter( db.auth_membership.user_id ), -- would require left join...
        queryFilter( Field('user', 'integer', 
                     requires=IS_IN_DB(db, 'auth_user.id',  db.auth_user._format)), 
                     target_expression = db.auth_user.id
                    ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name] ,
                 )     

def test8_aggregate(): # OK;   TODO: automatically detect if target_is_aggregate
    
    search = SearchForm(
        queryFilter( Field( "count_user_groups", 'integer'), '>', target_expression=db.auth_group.id.count(), target_is_aggregate=True ),
        queryFilter( db.auth_user.first_name ),
        # queryFilter( db.auth_group.role )
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_group.id.count() ] ,
                    left = build_joins_chain([ db.auth_user, db.auth_membership, db.auth_group ]),
                    groupby=db.auth_user.first_name , 
                 )     


def testgrand_technology_with_good():
    # db.technology.sku.name = "bla.bla"  # IGNORUOJA laukus, su tašku pavadinime

    search = SearchForm(
        queryFilter( db.technology.active ),
        queryFilter( db.technology.sku, '==' ),
        queryFilter( db.technology.title, 'contains' ),
        queryFilter( db.technology.type ),
        queryFilter( db.technology.good_id ),
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
    


