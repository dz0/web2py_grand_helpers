# -*- coding: utf-8 -*-
# 
from gluon.storage import Storage
from gluon import current
# from gluon import *
# from gluon.html import xmlescape
from gluon.sqlhtml import SQLFORM
from pydal.objects import Field, Row, Expression
from collections import defaultdict

from pydal._globals import DEFAULT

DEFAULT_TABLE_NAME = 'AnySQLFORM'

# test fields
orphan_with_target = Field('orphan_name')
orphan_with_target.target_expression = db.auth_user.last_name + "bla"
def test_fields():
  return [

        db.auth_user.first_name, 
        db.auth_user.last_name, 
        
        db.auth_user.id,
        db.auth_group.role, # db.auth_group.description,
        
        FormField(db.auth_permission.table_name),
        SearchField(db.auth_permission.id),
        
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
        FormField( db.auth_user.first_name + db.auth_user.last_name, name='full_name'),
        FormField( Field( 'pure_inputname_in_form'), name_extension='', prepend_tablename=False, target_expression='pure' ),  
    ]
    
def test_queryform():
    fields = test_fields() 
    
    # db.auth_permission._format = "%(name)s %(table_name)s (%(id)s)"

    form = QuerySQLFORM( *fields )
    # form = SQLFORM.factory( *fields )

    form.process(keepvalues=True)
    data = form.vars_as_Row()
    query = form.build_queries()
        
    return dict(
            query = query,
            form=form, 
            data_dict = repr(data),
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

    form.process(keepvalues=True)
    data = form.vars_as_Row()
        
    return dict(
            form=form, 
            data_dict = repr(data),
            data = data,
            vars=form.vars,
            vars_dict=repr(form.vars),
            )
    



################## module ##################

def find_out_attr(attr, data_sources):
    for data in data_sources:
        # dict obj
        if isinstance(data, dict)  and  attr in data:   return data[attr]
        # or object property
        elif hasattr( data, attr ): return getattr(data, attr) 
    raise KeyError( "%s not found in data_sources %s" % (data, data_sources))
    

default_field_attrs = dict( 
                type='string',
                length=None,
                default=DEFAULT,
                required=False,
                requires=DEFAULT,
                ondelete='CASCADE',
                notnull=False,
                unique=False,
                uploadfield=True,
                widget=None,
                label=None,
                comment=None,
                writable=True,
                readable=True,
                update=None,
                authorize=None,
                autodelete=False,
                represent=None,
                uploadfolder=None,
                uploadseparate=False,
                uploadfs=None,
                compute=None,
                custom_store=None,
                custom_retrieve=None,
                custom_retrieve_file_properties=None,
                custom_delete=None,
                filter_in=None,
                filter_out=None,
                custom_qualifier=None,
                map_none=None,
                # rname=None
                 )
# this should make kind of layer over Field
class FormField( Field ):
    """
    ovverides name property to include table_name
    New property: target_expression
    """
        
    def __init__(self, field, **kwargs):
        """ field is of type Field or Expression
        """
#         if str(field).startswith('user_id'):
#             pass
 
        if not isinstance(field, Expression):
            # if first argument is not Expression
            if 'target_expression' in kwargs: 
                field = Field( fieldname=field ) # workaround if we get just name instead of field
                field.target_expression = kwargs.pop('target_expression')
            elif 'name' in kwargs:
                tmp_target_expression = field
                field = Field( kwargs['name'] )
                field.target_expression = Field( tmp_target_expression ) # tmp_target_expression # Field( tmp_target_expression ) is a weird workaround...
            elif isinstance(field, str):  # in case just one string as args
                #tmp_target_expression = field
                field = Field( fieldname=field ) 
                field.target_expression = field # tmp_target_expression
            else:
                raise ValueError("Wrong argument for FormField(..): %s" % field )
                
 
        # try to infer TARGET_EXPRESSION and TABLENAME
        
#         if 'user_id' in str(field):
#             print 0
        # New PROPERTY (to remember original field/expression)
        if hasattr(field, 'target_expression'):  # if  isinstance(field, FormField) or
            self.target_expression = field.target_expression
            
        else:
            self.target_expression = field  # we leave direct connection to the field -- for data to be compared/inserted


        if isinstance(self.target_expression, Expression):
            self.type = self.target_expression.type
        else: 
            type_map = {int:'integer', long:'integer', float:'double',
                        str:'string',  unicode:'string'}
            
            self.type = type_map.get( type(self.target_expression), 'string')  # for str, int, etc

        # assign table_name if not present
        if isinstance(field, Field):   # if not bare Expression, assign some tablename
            if not hasattr(field, 'tablename'):
                field.tablename = field._tablename  = 'no_table'
            self.tablename = self._tablename = field.tablename    



        # in rare cases    
        default_field_attrs['rname'] = getattr(field, '_rname', None)

        # populate attrs for Field constructor
        data_srcs = [kwargs, self, field, default_field_attrs]
        field_attrs = { key: find_out_attr(key, data_srcs)   for key in default_field_attrs }
            
        # call Super init
        new_name = self.construct_new_name( field, kwargs ) 
        Field.__init__(self, fieldname=new_name, **field_attrs)

        if type(field) is Field:
            self.label = self.tablename + ': ' +self.label
        
        self.__dict__.update( kwargs )

        # some polishing for  target_expresion  (if it is Field)
        # TODO: for Expression we could also construct IS_IN_SET or so..  (with distinct)
        if isinstance(self.target_expression, Field):
            if self.target_expression.tablename == 'no_table':
                self.target_expression = self.target_expression.name  # if field is orphan, so we remember just its name

            if self.tablename == 'no_table': 
                pass # TODO:  aliases?

            if hasattr(self, 'type'):
                # f =  self.target_expression
                
                # if getattr(self, 'tablename', 'no_table') == 'no_table': # if orphan field
                if self.tablename == 'no_table': # if orphan field
                    
                    # we infer requires  from  FK
                    if self.type.startswith('reference ') or self.type.startswith('list:reference '): 
                        foreign_table = self.type.split()[1]
                        # self.target_expression = db[foreign_table]._id  # probably better no, as looses info 
                        self.requires = IS_EMPTY_OR( IS_IN_DB( db, db[foreign_table], db[foreign_table]._format ) )
    
                
                if self.type == 'id':  # override, as otherwise field is not shown in form
                    self.type = 'integer'
                    self.table = db[self.tablename]
                    self.requires = IS_EMPTY_OR( IS_IN_DB( db, self.table, self.table._format ) )
         
            
    
    def construct_new_name(self, field, kwargs ):
        """ also defaults self and field .tablename  to  "no_table" if not present """
        
        
        if 'name' in kwargs:  
            new_name = kwargs.pop('name')   # required for  Expression which is not Field
        else:

            if isinstance(field, FormField):  # we already constructed the name earlier
                self.name = new_name = field.name  
                return self.name
                """
                #if hasattr(field, '__initial_name'):
                if type(field.target_expression) == Field:
                    new_name = field.target_expression.name
                # elif isinstance(field.target_expression,  str):
                #    new_name = str(field.target_expression)
                #elif field._FormField__initial_name:
                    #new_name == field._FormField__initial_name  # Mystycs: UnboundLocalError: local variable 'new_name' referenced before assignment
                """

            else:  # for Field  (or Expression with name property)
                new_name = field.name  # this is absent in Expression
                

        if type(field) == Field:   #  isinstance would be bug
            # if not hasattr(field, 'tablename'):
                # field.tablename  = 'no_table'
            if kwargs.pop('prepend_tablename', True):   # hack to let fieldnames stay as they are
                new_name = field.tablename+"__"+new_name
        
        
        #self.__initial_name = getattr(field, 'name', None)
        self.name = new_name
        return new_name
         
    def get_query(self, val):
        return self.target_expression == val
        
        
class AnySQLFORM( ):  
    """Works as proxy """
    def __init__(self, *fields,  **kwargs ):  #
        # SQLFORM.__init__(self, *fields, **kwargs )
    
        field_decorator = kwargs.pop('field_decorator', FormField)
        self.formfields = []
        self.structured_fields = self.traverse_fields( fields, self.formfields, field_decorator  )
        # self.formfields = [f if isinstance(f, FormField) else FormField(f) for f in fields ]
        
        # factory could be SQLFORM.factory or SOLIDFORM.factory or so..
        form_factory= kwargs.pop('form_factory', SQLFORM.factory)
        self.table_name  = kwargs.pop('table_name',  DEFAULT_TABLE_NAME)
        self.__form = form_factory ( *self.structured_fields, table_name=self.table_name, **kwargs )

    @staticmethod
    def traverse_fields( items, flattened, field_decorator  ):  # works recursively        
        """ is needed for SOLIDFORM, because it gets  *fields  matrix,    not flat list 
        Recursivelly makes fields FormField (or so) instances and generated flattened list
        """
        result = []
        for nr, item in enumerate(items):
            
            if isinstance(item, (list, tuple)):  # if we get a list
                result.append( traverse_fields( item , flattened, field_decorator ))  # apply recursively
                
            else:      # simple field
                
                #  make FormField instance if needed
                if not isinstance( item, field_decorator  ):  
                    item = field_decorator ( item ) # apply with defaults
                    
                # item.writable = True
                # item.readable = True            
                result.append( item )
                # print( "DBG field name:", item.name )
                flattened.append( item )
        
        return result

    
    def __getattr__( self, name ):
        if name in  self.__dict__:
            return  self.__dict__[ name ]
        else:
            return getattr( self.__form, name )      

    def get_field(self, arg, **kwargs ):
        rez = []
        for f in self.formfields:
            if isinstance(arg, str):
                if f.name == arg:
                    rez.append(  f )

            if isinstance(arg, Expression):  # TODO: what if target_expression is direct value (int, str)?
                def match_kwargs(): # helper 
                    for key, val in kwargs.items():
                        if (not hasattr(f, key)
                        or getattr(f, key) != val):
                            return False
                    return True
                    
                if f.target_expression == arg and match_kwargs():
                    rez.append(  f )
        
        if len(rez)==1:
            return rez[0]
        
        return rez
        # elif len(rez)>1:
            # return rez
        # else:
            # raise KeyError("FormField '%s' not found" % arg)
        
    def get_value(self, field, vars=None):
        
        if not vars:
            # default to self.vars
            if not self.vars: self.process(keepvalues=True) # or self.__form.process()
            vars = self.vars

        # return current.request.vars.get( field.name, None ) 
        
        return vars[field.name] # could be request vars
        

    def vars_as_Row( self, vars=None ): # but types are not checked/converted
        row = defaultdict( dict )
        
        # if vars is None:
            # vars = current.request.vars

        for f in self.formfields:
            # if f.name in vars:  # this would cause some of stuff missing..
                # value = vars[f.name]
                value = self.get_value( f )
                expr = f.target_expression  # todo -- what if it is missing?
                # print "DBG expr", expr
                if type(expr) is Field:
                    row[expr.tablename][expr.name] = value
                elif isinstance(expr, Expression): # not  Field
                    row['_extra'][str(expr)] = value
                else : # should be no_table stuff
                    # row['_direct_values'][str(expr)] = value
                    # row['_direct_values'][expr] = value
                    row['_no_table'][expr] = value
        
        return Row( **row ) 
        

#########################################                    SEARCH      ##############################################

"""  
# memo  from sqlhtml.py class grid
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
class SearchField( FormField ):
    """New properties: comparison
    New methods: get_query, overrides  construct_new_name
    """
                         
    def __init__(self, field, **kwargs):
        """ field is of type Field or Expression
        Expects params: 
           query_function
           target_is_aggregate
        """
        
        FormField.__init__(self, field, **kwargs ) 
        
        # self.query_function = kwargs.pop('query_function', None)  # should be get from super __init__
        
        self.label += " (%s)"%self.name_extension  # DBG

        from gluon.validators import Validator , IS_EMPTY_OR
        if isinstance( self.requires , Validator):
            #print( name, field.requires )
            self.requires = IS_EMPTY_OR( self.requires )
            
        # assign needed properties    
        #self.target_is_aggregate = kwargs.get('target_is_aggregate', None)  # might be used in SearchFORM build_queries
        #self.query_function = kwargs.get('query_function', None)  # might be used in SearchFORM build_queries

        for key in ['target_is_aggregate', 'query_function']:
            if key in kwargs:
                val = kwargs[key]
            else:
                val  = getattr(self, key, None) # maybe we already had property (in some way)
            setattr(self, key, val)

    def init_comparison_and_name_extension(self, field, kwargs):
        # comparison
        if not hasattr(self, 'comparison'):
            self.comparison = kwargs.pop( 'comparison',  None )
        
        if not self.comparison:
            self.comparison = '='
#            if not isinstance( self.target_expression, str):
#             if  isinstance( self.target_expression, Expression):     
            if field.type in ('text', 'string', 'json'):
                if self.comparison == '=': 
                    # comparison = 'like'     # a bit smarter ;)
                    self.comparison = 'contains'     # a bit smarter ;)


        # name extension (based on comparison)
        extensions = {   '!=': 'not_equal',
                         '<' : 'less_than',
                         '<=': 'less_or_equal',
                         '=' : 'equal',
                         '>' : 'greater_than',
                         '>=': 'greater_or_equal',
                        }

#         if not hasattr(self, 'name_extension'):
#             self.name_extension = kwargs.pop('name_extension', None)
           
        try:
            self.name_extension = find_out_attr('name_extension', [self, kwargs, field])
        except KeyError, AttributeError:    
            # if self.name_extension is None:
            self.name_extension = extensions.get(self.comparison, self.comparison)  
       
               
    def construct_new_name( self, field, kwargs ):
        new_name = FormField.construct_new_name( self, field, kwargs )
        self.init_comparison_and_name_extension( field, kwargs )

        if self.name_extension: # we could give      name_extension = ''  (in kwargs), so it is not appended
            new_name += '__'+self.name_extension  

        return new_name
        
    def get_query(self, val):
        if self.query_function:
            return self.query_function( val )
            # return self.query_function( self, val ) # or so? because query_function  is monkeypatched
        
        else:
            def query4filter_direct_val(expr, op, value):
                "woraruond to see not result (True/False: 1/0) but expression in sql query"
                # http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html
                # https://www.tutorialspoint.com/postgresql/postgresql_operators.htm
                # https://www.tutorialspoint.com/sqlite/sqlite_comparison_operators.htm
                if op == '==': op = '='
                for o in "= < > <= >= !=":
                    if op == o:
                        return  "%s %s %s" %( repr(expr), op, repr(value) ) 
                if isinstance(value, (str)): 
                    # http://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
                    # https://www.postgresql.org/docs/8.3/static/functions-matching.html
                    # https://www.sqlite.org/lang_expr.html :)
                    if op == 'contains': fmt="%%%s%%"
                    elif op == 'like': fmt="%s"
                    elif op == 'startswith': fmt="%%%s"
                    elif op == 'endswith': fmt="%s%%"
                    return ("LOWER('%s') LIKE '"+ fmt ) % (expr, val.lower() )    #TODO FIXME sqlite +"' ESCAPE '\'"     
                raise RuntimeError("Invalid operation: %s %s %s" %(repr(expr), op, repr(value)) )
            
                
            def query4filter(expr, op, value):
                # taken from pydal/helpers/methods.py  def smart_query
                # in general should map:
                # field = target_expression  (Field or expression of Fields)
                # op = comparison
         
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

            if isinstance(self.target_expression, (str, int,  float, long,  )):
                return query4filter_direct_val(self.target_expression, self.comparison, val)
            else: 
                return query4filter(self.target_expression, self.comparison, val)

        
        
class QuerySQLFORM (AnySQLFORM ):
    
    def __init__(self, *fields,  **kwargs ):
        """
        if multiple tables will be in context
        should get parameter "join_chains", which is matrix of tablenames, each row represents some logicat join chain for "left" argument
        """
        # for data model/view construction -- would be reused in build_queries and  field validators IS_IN_DB
        for key in 'left, join_chains, orderby, groupby, distinct'.split(', '):
            setattr(self, key, kwargs.pop(key, None) )
            
        
        if self.join_chains and not self.left:
            self.left = []
            for table_list in self.join_chains:
                self.left.extend( build_join_chain( table_list ) )
                
        AnySQLFORM.__init__(self, *fields, field_decorator=SearchField, **kwargs)
        # self.formfields = [f if isinstance(f, SearchField) else SearchField(f) for f in fields ]

        
    def build_queries(self, ignore_orphaned_fields=False):
        queries = []
        queries_4aggregates = []
        # for filter in flattened_filters:
        for f in self.formfields:
            input_value =  self.get_value( f )  # gets the input value
#             print "DBG ", f
            # if 'some' in f.name:
                # print "DBG", f
            expr = f.target_expression
            if isinstance(expr, Field) and expr.tablename == 'no_table':
                if ignore_orphaned_fields:
                    continue
                else:
                    raise TypeError("not possible to construct query with orphaned field: %s (form field name: %s) -- should be provided with target_expression" % (expr, f.name)) 
            #if "some" in f.name:
            #    print "dbg, target", f.target_expression
            if input_value:
                q = f.get_query( input_value ) # produce query

                db = getattr(f.target_expression, 'db', None) # target expression might be str type
                if db and f.target_expression.op in [db._adapter.AGGREGATE, db._adapter.COUNT]:
                # or db._adapter.dialect and f.target_expression.op in [db._adapter.dialect.AGGREGATE, db._adapter.dialect.COUNT]:  # for newer pydal... untested
                    f.target_is_aggregate  = True # overrides default                    
                
                if f.target_is_aggregate:  # with this works OK
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
            
        return Storage( query=query, having=having )

    # @property
    # def query(self):
        # return self.build_queries().query
        
    # @property
    # def having(self):
        # return self.build_queries().having
        



####### DALSELECT ##########
from plugin_joins_builder.joins_builder import build_joins_chain , get_referenced_table # uses another grand plugin

# from pydal/adapters/base.py
SELECT_ARGS = (
     'orderby', 'groupby', 'limitby', 'required', 'cache', 'left', 'distinct',
     'having', 'join', 'for_update', 'processor', 'cacheable',
     'orderby_on_limitby'
     )

class DalView(Storage):
    """similar as DB set, but "packs" query into kwargs 
    and adds join_chains property (which can infer some usefull info for ReactiveSQLFORM)
    """
    
    def kwargs_4select(self):
        return {key:self[key] for key in SELECT_ARGS if self[key]}
        
    def __init__(self, *fields, **kwargs):
        """
        important part is join_chains -- array of join_chain (see plugin_joins_builder) 
                         they can be reused by reactiveFORM... to figure out which tables' fields should be updated  
                         
        ps.:  "fields" mean more generally "columns" or "expressions". But for consistency I leave as "fields"...
        """
        self.fields = fields
        
        for key in SELECT_ARGS+('query', 'left_join_chains', 'inner_join_chains'):
            self[key] = kwargs.get(key)
                    
    
        if self.left and self.left_join_chains :
            raise RuntimeError("Overlapping args for left...join_chains, %s" % self.left_join_chains)
            
        if self.join and self.inner_join_chains :
            raise RuntimeError("Overlapping args for inner...join_chains, %s" % self.inner_join_chains)
        
        if not self.left :
            self.get_join('left') # default
            
        if not self.join :
            self.get_join('inner')
            
    def get_join_chains( type_ = 'left'):
        #parse chains and return tablenames
        return "TODO" 
        
    def get_join(self, type_='left'): # TODO: better make left as @property
        #its a pitty, that there is left and join, but not left and inner properties...
            
        if type_=='left':
            if not self.left : 
                self.left = []
                if self.left_join_chains:
                    for jchain in self.left_join_chains:
                        self.left.extend( build_joins_chain(  *jchain ) )
            return self.left 
              
        if type_=='inner':
            if not self.join : 
                self.join = []
                if self.inner_join_chains:
                    for jchain in self.inner_join_chains:
                        self.join.extend( build_join_chain(  jchain ) )
            return self.join      


    def guarantee_table_in_query(self):
        if self.query == True:
            main_table = self.fields[0].table
            self.query = main_table.id > 0

    def get_sql(self):
        self.guarantee_table_in_query()
        return db(self.query)._select( *self.fields, **self.kwargs_4select() )
        
    def execute(self): # usuall select
        self.guarantee_table_in_query()
        return db(self.query).select( *self.fields, **self.kwargs_4select() )
        
    def get_grid_kwargs(self):
        return "TODO"
        
    def __call__(self):
        return self.execute()
        

def get_expressions_from_formfields( formfields, include_orphans = False ):
    # result = []
    # for f in formfields:
    #     if isinstance(f, FormField):
    #         result.append( f.target_expression )
    #     else:
    #         result.append( f )
    result =  [f.target_expression if isinstance(f, FormField) else f     for f in formfields ]
    if not include_orphans:
        result = [expr for expr in result
                        if isinstance(expr, Field) and getattr(expr, 'tablename', 'no_table') != 'no_table'
                        or type(expr) is Expression
                  ]
    return result

def test_dalview():

    fields = test_fields() 
    
    form = QuerySQLFORM( *fields )

    form.process(keepvalues=True)
    query_data = form.vars_as_Row()
    filter = form.build_queries()
    
    cols =get_expressions_from_formfields(fields)
    print "dbg cols", cols


    main_table = fields[0].table
    # search.query &= (db.auth_user.id < 5)  
    if filter.query==True: filter.query = main_table.id > 0

    sel = DalView(*cols, query=filter.query, having=filter.having,
                  left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                  )

    sql = sel.get_sql()
    print( "DBG SQL: ", sql )

    

    # GRID
    data = None
    # data = SQLFORM.grid(search.query, fields=selected_fields, **kwargs )  # can toggle
    # data = get_data_with_virtual()
    data = sel.execute()
    

        
    return dict(
            sql = sql,
            filter = filter, # query and having
            form=form, 
            # data_repr = repr(data),
            data = data,
            vars=form.vars,
            # vars_dict=repr(form.vars),
            )    

######## 
class ReactiveSQLFORM():
    def callback():
        pass

#
# GrandDalView -- include translations
class GrandRegister( Storage ):
    """
    search with register (and translations)
    should look sth like:
    >>> GrandRegister(
        columns = [ ],
        left_join_chains = [ [] ], # probably would be enough
        search_fiels = [ ], 
        search_fields_update_triggers = {None:[ ]}
        translate_fields = [],
    )
    
    mostly used for:
       form
       filter: query, having
       records_w2ui
    
    """
    def __init__( self, *columns, **kwargs ):

        self.columns  = columns or kwargs.pop('columns')
        self.cid =  kwargs.pop('cid', current.request.function )

        self. update( kwargs )

        self.search_form = QuerySQLFORM( *self.search_fields, form_factory=SQLFORM.factory )
        self.search_fields = self.search_form.formfields  # UPDATES items to SearchField instances

        # self.left_join_chains = self.join_chains or [[]]
        # self.search_fiels = self.search_fiels or columns

        # self.search_fields_update_triggers                    # TODO: for ReactiveForm
        # self.translate_fields                               # TODO: for GrandTranslator



    def w2ui_grid(self):
        # some workarounds for grand core stuff
        request = current.request

        response.subtitle = "test  w2ui_grid"
        response.menu = []

        context = dict(
            cid = self.cid,
            w2grid_columns=[
                {'field': FormField(f).name, 'caption': f.label, 'size': "100%",
                 'sortable': isinstance(f, (Field, Expression)), 'resizable': True}
                    for f in self.columns
                ],
            grid_function=request.function,  # or 'users_grid'
            data_name=self.data_name or request.controller,
            # w2grid_sort = [  {'field': w2ui_colname(db.auth_user.username), 'direction': "asc"} ]
            w2grid_sort=[{'field': FormField(self.columns[0]).name, 'direction': "asc"}]
            # ,dbg = response.toolbar()
        )
        return context

    def form_register(self):
        # cid?
        self.search_form.process(keepvalues=True)

        return dict(search_form=self.search_form, w2ui_grid = self.w2ui_grid() )

    # def search_filter(self):
    #     " query and having "
    #     return self.search_form().build_queries()


    # # SIMPLE DATA -- ok for w2ui_records
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

    def records_w2ui(self):

        filter = self.search_form().build_queries()

        selection = DalView(*self.columns, query=filter.query, having=filter.having,
                      left_join_chains=self.join_chains,
                      )

        rows = self.selection.execute()
        # return rows.render()

        return rows.render()


def test_grandform():
    search_fields = test_fields()
    cols = get_expressions_from_formfields(search_fields )

    register = GrandRegister(cols,
                             search_fields = search_fields ,
                             left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]],
                             )

    # response.view = ...
    if request.vars.grid:
        response.view = "generic.json"
        return register.records_w2ui()

    else:
        # response.view = "plugin_w2ui_grid/w2ui_grid.html"
        return register.form_register()



