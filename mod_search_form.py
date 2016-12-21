from gluon.storage import Storage
from gluon import current
from gluon.sqlhtml import SQLFORM


########################################
#    SEARCH FILTERS QUERY from FORM    #
########################################

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
                query_function=None #  function which expects value, and builds query ( target_expr op value ) 
                , target_is_aggregate=False
                ):
    """
    field --  db.table.field  # input field
    comparison -- action as in grid search fields (str)
    name_extension -- if we use same field twice or more we need to name them different -- by default based on comparison operation 
    target_expression -- to what we want to compare our value
    query_function -- should be contained in lambda, because   value for comparison  is not known at the  creation time
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


    from gluon.validators import Validator , IS_EMPTY_OR
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
    
    if not query_function:
        # def query_function( value ): return query4filter(field, comparison, value)
        query_function = lambda value: query4filter(target_expression, comparison, value)
    
    return Storage(
                    field=search_field, 
                    # name=name,   # is in field -- so maybe not needed
                    query_function=query_function,
                    # comparison=comparison,               # is in query_function'je, so maybe not needed
                    target_expression=target_expression  # is in query_function'je, so maybe not needed
                    ,target_is_aggregate = target_is_aggregate
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
        filter_value =  current.request.vars.get( filter.field.name, None )
        if filter_value:
            q = filter.query_function( filter_value ) # produce query
            
            # if filter.target_expression.op == db._adapter.AGGREGATE:
            if filter.target_is_aggregate:
                # print("DBG db.adapter", dir(db._adapter))
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



