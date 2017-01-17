# -*- coding: utf-8 -*-
# skBMLYp0
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
        
        FormField(db.auth_permission.table_name),
        SearchField(db.auth_permission.id),
        
        # no_table items   
        Field('user_id', 'reference auth_user'), 
        
        # Field( 'somefield' )),  # for AnySQLFORM   Field( 'somefield' ) would be enough
        FormField( Field( 'somefield' ), target_expression='ha' ),  # for AnySQLFORM   Field( 'somefield' ) would be enough
        orphan_with_target,
        # expression (as target)
        FormField( db.auth_user.first_name + db.auth_user.last_name, name='full_name'),
    ]
    
def test_searchform():
    fields = test_fields() 
    
    # db.auth_permission._format = "%(name)s %(table_name)s (%(id)s)"

    form = SearchSQLFORM( *fields )
    # form = SQLFORM.factory( *fields )

    form.process(keepvalues=True)
    data = form.vars_as_Row()
    query = form.build_query()
        
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
 
        # try to infer TARGET_EXPRESSION and TABLENAME
#         if 'user_id' in str(field):
#             print 0
        # New PROPERTY (to remember original field/expression)
        if hasattr(field, 'target_expression'):  # if  isinstance(field, FormField)
            self.target_expression = field.target_expression
        else:
            self.target_expression = field  # we leave direct connection to the field -- for data to be compared/inserted

            if getattr(field, 'tablename', 'no_table') == 'no_table': # if orphan field
                # TODO:  aliases?
                
                # we infer target_expression from  FK
                if field.type.startswith('reference ') or field.type.startswith('list:reference '): 
                    foreign_table = field.type.split()[1]
                    self.target_expression = db[foreign_table]._id
                    self.requires = IS_IN_DB( db, db[foreign_table], db[foreign_table]._format ) 


        # assign table_name if not present
        if isinstance(field, Field):   # if not bare Expression, assign some tablename
            if not hasattr(field, 'tablename'):
                field.tablename = field._tablename  = 'no_table'
            self.tablename = self._tablename = field.tablename    


        # in rare cases    
        default_field_attrs['rname'] = getattr(field, '_rname', None)

        # populate attrs for field constructor
        def find_out_attr(attr):
            # 1st) look in kwargs
            if attr in kwargs: return kwargs.pop(attr)
            # 2nd) maybe attr was already set to self  in previous code  
            elif hasattr( self, attr ):  return getattr(self, attr)  # delete it from kwargs as well
            # 3rd) look in field attrs
            elif hasattr( field, attr ): return getattr(field, attr)
            # else
            else: return default_field_attrs[attr]

        field_attrs = { key: find_out_attr(key)   for key in default_field_attrs }
            
    
            
        # call Super init
        new_name = self.construct_new_name( field, kwargs ) 

        Field.__init__(self, fieldname=new_name, **field_attrs)

               
        if isinstance(field, Field):
            self.label = self.tablename + ': ' +self.label
        
        self.__dict__.update( kwargs )
        
        if field.type == 'id':  # override, as otherwise field is not shown in form
            self.type = 'integer'
            self.requires = IS_IN_DB( db, field.table, field.table._format ) 
         
         
    
    def construct_new_name(self, field, kwargs ):
        """ also defaults self and field .tablename  to  "no_table" if not present """
        
        if 'name' in kwargs:  
            new_name = kwargs.pop('name')   # required for  Expression which is not Field
        else:
            new_name = field.name  # this is absent in Expression
            
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
        factory_class= kwargs.pop('factory_class', SQLFORM)
        self.table_name  = kwargs.pop('table_name',  DEFAULT_TABLE_NAME)
        self.__form = factory_class.factory ( *self.structured_fields, table_name=self.table_name, **kwargs )

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

            if isinstance(arg, Expression):
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
        
    def get_value(self, field):
        # if not self.vars:  # some singleton
            # self.process(keepvalues=True) # or self.__form.process()
        # return self.vars[field.name]
        return current.request.vars.get( field.name, None )

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
                elif isinstance(expr, Expression): # not Field
                    row['_extra'][str(expr)] = value
        
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
        #self.target_is_aggregate = kwargs.get('target_is_aggregate', None)  # might be used in SearchFORM build_query
        #self.query_function = kwargs.get('query_function', None)  # might be used in SearchFORM build_query

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
            if not isinstance( self.target_expression, str): 
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

        self.name_extension = kwargs.pop('name_extension', None)
        if self.name_extension is None:
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
                    return ("LOWER('%s') LIKE '"+ fmt ) % (expr, lower(val))    #TODO FIXME sqlite +"' ESCAPE '\'"     
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

        
        
class SearchSQLFORM (AnySQLFORM ):
    
    def __init__(self, *fields,  **kwargs ):
        """
        if multiple tables will be in context
        should get parameter "join_chains", which is matrix of tablenames, each row represents some logicat join chain for "left" argument
        """
        # for data model/view construction -- would be reused in build_query and  field validators IS_IN_DB
        for key in 'left, join_chains, orderby, groupby, distinct'.split(', '):
            setattr(self, key, kwargs.pop(key, None) )
            
        
        if self.join_chains and not self.left:
            self.left = []
            for table_list in self.join_chains:
                self.left.extend( build_join_chain( table_list ) )
                
        AnySQLFORM.__init__(self, *fields, field_decorator=SearchField, **kwargs)
        # self.formfields = [f if isinstance(f, SearchField) else SearchField(f) for f in fields ]

        
    def build_query(self, ignore_orphaned_fields=False):
        queries = []
        queries_4aggregates = []
        # for filter in flattened_filters:
        for f in self.formfields:
            input_value =  self.get_value( f )  # gets the input value
#             print "DBG ", f
            if 'some' in f.name:
                print "DBG", f
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
        # return self.build_query().query
        
    # @property
    # def having(self):
        # return self.build_query().having
        
