# -*- coding: utf-8 -*-
from gluon.storage import Storage


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
    

def test_2_same_fields(): # OK
    search = searchQueryfromForm(
        queryFilter( db.auth_user.first_name, '=' ),
        queryFilter( db.auth_user.first_name, 'contains')
    )
    return tester(  search, 
                    selected_fields=[db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 


def test_expr_combination_of_fields(): # OK...
    search = searchQueryfromForm(
        # queryFilter( db.auth_user.first_name, 'contains' ),
        queryFilter( Field( "first_name_with_email"),  target_expression=db.auth_user.first_name + db.auth_user.email ),
        queryFilter( db.auth_user.email ),
    )
    
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 
    
    
def test_expr_simple_field(): # OK
    search = searchQueryfromForm(
        # queryFilter( db.auth_user.first_name, 'contains' ),
        queryFilter( Field( "first_name__custom"),  target_expression=db.auth_user.first_name  ),
        queryFilter( Field( "first_name__custom2"), '<',  target_expression=db.auth_user.first_name  ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_user.email ] 
                 ) 

def test_reference_by_anonymous_field(): # OK
    
    search = searchQueryfromForm(
        # queryFilter( db.auth_membership.user_id ), -- would require left join...
        queryFilter( Field('user', 'integer', 
                     requires=IS_IN_DB(db, 'auth_user.id',  db.auth_user._format)), 
                     target_expression = db.auth_user.id
                    ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name] ,
                 )     


def test_reference_field_widget(): # OK
    from mod_joins_builder import build_joins_chain, set_db;  set_db(db)
    
    search = searchQueryfromForm(
        queryFilter( db.auth_membership.user_id ),
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_group.role ] ,
                    left = build_joins_chain([ db.auth_user, db.auth_membership, db.auth_group ]),
                 )     


def test_aggregate(): # OK;   TODO: automatically detect if target_expression_is_aggregate
    from mod_joins_builder import build_joins_chain, set_db;  set_db(db)
    
    search = searchQueryfromForm(
        queryFilter( Field( "count_user_groups", 'integer'), '>', target_expression=db.auth_group.id.count(), target_expression_is_aggregate=True ),
        queryFilter( db.auth_user.first_name ),
        # queryFilter( db.auth_group.role )
    )
    return tester(  search, 
                    selected_fields=[ db.auth_user.id, db.auth_user.first_name, db.auth_group.id.count() ] ,
                    left = build_joins_chain([ db.auth_user, db.auth_membership, db.auth_group ]),
                    groupby=db.auth_user.first_name , 
                 )     


def test_grand_technology_with_good():
    # db.technology.sku.name = "bla.bla"  # IGNORUOJA laukus, su tašku pavadinime

    search = searchQueryfromForm(
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
    


########################################
#    SEARCH FILTERS QUERY from FORM    #
########################################
if "SEARCH FILTERS QUERY from FORM":
    # memo  from sqlhtml.py class grid
    """
    search_options = {
        'string': ['=', '!=', '<', '>', '<=', '>=', 'starts with', 'ends with', 'contains', 'in', 'not in'],
        'text': ['=', '!=', '<', '>', '<=', '>=', 'starts with', 'contains', 'in', 'not in'],
        'date': ['=', '!=', '<', '>', '<=', '>='],
        'time': ['=', '!=', '<', '>', '<=', '>='],
        'datetime': ['=', '!=', '<', '>', '<=', '>='],
        'integer': ['=', '!=', '<', '>', '<=', '>=', 'in', 'not in'],
        'double': ['=', '!=', '<', '>', '<=', '>='],
        'id': ['=', '!=', '<', '>', '<=', '>=', 'in', 'not in'],
        'reference': ['=', '!='],
        'boolean': ['=', '!=']}
    """

              
    def queryFilter(field=None, comparison=None, name_extension=None,  # for smart way
                    
                    # for customisation
                    name=None,  #  name of input -- to ovveride field name
                    #input=None, #  INPUT(..)  # TODO -- if Field is not enough.. for input..
                    target_expression=None, # in case we use field of 'no_table' (this indicates the comparison target)
                    lambda_query=None #  lambda, which expexts value from Expr to be given as filtering query
                    , target_expression_is_aggregate=False
                    ):
        """
        field --  db.table.field  # input field
        comparison -- action as in grid search fields (str)
        name_extension -- if we use same field twice or more we need to name them different -- by default based on comparison operation 
        target_expression -- to what we want to compare our value
        lambda_query -- should be contained in lambda, because   value for comparison  is not known at the  creation time
        """
        
        # comparison
        if not comparison:
            comparison = '='
            if field.type in ('text', 'string', 'json'):
                if comparison == '=': 
                    # comparison = 'like'     # a bit smarter ;)
                    comparison = 'contains'     # a bit smarter ;)
            
        # prefix
        prefixes = { b:a.strip().replace(' ', '_') for a, b in 
                        [
                            ('less or equal','<='),
                            ('greater or equal','>='),
                            ('not equal','!='),
                            ('equal','='),
                            ('less than','<'),
                            ('greater than','>'),
                        ]
                    }
        if name_extension is None:
            name_extension = prefixes.get(comparison, comparison)  
        
            
        # name
        name = name or str(field).replace('.', '__').replace("<no table>", "no_table")
        if name_extension:
            name += '__'+name_extension  
             
        search_field = field.clone() # APPLY new name for search fields , but leave original field untouched             
        search_field.name = name
        search_field.label += " (%s)"%name_extension  # DBG
        #search_field.comment = name_extension


        from gluon.validators import Validator 
        if isinstance( search_field.requires , Validator):
            print( name, field.requires )
            search_field.requires = IS_EMPTY_OR( field.requires )
        # input = TODO # if widget not available or so.. # Should get into Form elements/conmponents mangling...
        
        
        target_expression = target_expression or field

        # query
        query_repr = None # for dbg purposes
        
        def query4filter(expr, op, value):
            # in general should map:
            # field = target_expression  (Field or expression of Fields)
            # op = comparison
            # if not type(field) is Field:
                # raise TypeError('%s is not "Field type"' % field)

            # elif not hasattr(field, '_tablename') or field._tablename=='no_table':
                # raise TypeError('%s does not have table specified "' % field)

            # taken from pydal/helpers/methods.py  def smart_query
     
            if op in ['=', '==']: new_query = expr==value; 
            elif op == '<': new_query = expr<value
            elif op == '>': new_query = expr>value
            elif op == '<=': new_query = expr<=value
            elif op == '>=': new_query = expr>=value
            elif op == '!=': new_query = expr!=value
            elif op == 'belongs': new_query = expr.belongs(value)
            elif op == 'notbelongs': new_query = ~expr.belongs(value)
            
            elif expr.type in ('text', 'string', 'json'):  
                if op == 'contains': new_query = expr.contains(value)
                elif op == 'like': new_query = expr.ilike(value)
                elif op == 'startswith': new_query = expr.startswith(value)
                elif op == 'endswith': new_query = expr.endswith(value)
                else: raise RuntimeError("Invalid operation: %s %s %s" %(expr, op, repr(value)) )
            elif expr._db._adapter.dbengine=='google:datastore' and \
                 expr.type in ('list:integer', 'list:string', 'list:reference'):
                if op == 'contains': new_query = expr.contains(value)
                else: raise RuntimeError("Invalid operation: %s %s %s" %(expr, op, repr(value)) )
            else: raise RuntimeError("Invalid operation: %s %s %s" %(expr, op, repr(value)) )
            
            return new_query
        
        if not lambda_query:
            # def lambda_query( value ): return query4filter(field, comparison, value)
            lambda_query = lambda value: query4filter(target_expression, comparison, value)
        
        return Storage(
                        field=search_field, 
                        # name=name,   # is in field -- so maybe not needed
                        lambda_query=lambda_query,
                        # comparison=comparison,               # is in lambda_query'je, so maybe not needed
                        target_expression=target_expression  # is in lambda_query'je, so maybe not needed
                        ,target_expression_is_aggregate = target_expression_is_aggregate
                      )
        
        
    def searchQueryfromForm(
            *filters
            # *components, **attributes
        ):
        # FORM
        fields = []
        formname = "Search_form_"
        for filter in filters:
            if filter.field:
                # filter.field.writable = True
                # filter.field.readable = True
                fields.append( filter.field )
                print( "DBG field name:", filter.field.name )
                formname += filter.field.name
            else:
                raise RuntimeError("need to define input")
        
        form = SQLFORM.factory(
            *fields, 
            keepvalues=True,
            table_name = "Search_form_",
            formname = formname
        )
        
        form.process(keepvalues=True)


            
        # BUILD QUERY    

        queries = []
        queries_4aggregates = []
        for filter in filters:
            filter_value =  request.vars.get( filter.field.name, None )
            if filter_value:
                q = filter.lambda_query( filter_value ) # produce query
                # if filter.target_expression.op == db._adapter.AGGREGATE:
                if filter.target_expression_is_aggregate:
                    queries_4aggregates.append(  q ) 
                else:
                    queries.append( q )
            
        # print('DBG queries_4aggregates:', queries_4aggregates)
        if queries:
            query = reduce(lambda a, b: (a & b), queries) 
        else:
            query = True # dummy query, which doesn't break anything  # would show "AND 1" in SQL 

        if queries_4aggregates:
            having = reduce(lambda a, b: (a & b), queries_4aggregates)
        else:
            having = None
        
        
        return Storage( form=form, query=query, having=having )


