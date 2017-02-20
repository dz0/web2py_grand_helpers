# -*- coding: utf-8 -*-

from gluon.storage import Storage
from gluon import current

from gluon.validators import Validator , IS_EMPTY_OR, IS_IN_DB, IS_IN_SET


from gluon.sqlhtml import SQLFORM
from pydal.objects import Field, Row, Expression
from collections import defaultdict

from pydal._globals import DEFAULT


DEFAULT_TABLE_NAME = 'AnySQLFORM'


################## module ##################

def find_out_attr(attr, data_sources, defaultsNone=False):
    for data in data_sources:
        # dict obj
        if isinstance(data, dict)  and  attr in data:   return data[attr]
        # or object property
        elif hasattr( data, attr ): return getattr(data, attr)

    if defaultsNone:
        return None

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

        db = self.db = current.db
        
        self.__dict__.update( kwargs )

        
        # some polishing

        # for  target_expresion  (if it is Expression)
        if type(self.target_expression) == Expression:
            self.target_expression.name = self.name  # if name is given to FormField -- forward it to target_expresssion -- though can overlap if several fields have same ...
            # self.target_expression.name = field_attrs.get('name')  # if name is given to FormField -- forward it to target_expresssion -- though can overlap if several fields have same ...
            self.target_expression.label = self.label


        # for  target_expresion  (if it is Field)
        if isinstance(self.target_expression, Field):

            # if field is orphan, so we remember just its name
            if self.target_expression.tablename == 'no_table':
                self.target_expression = self.target_expression.name

            if self.tablename == 'no_table': 
                pass # TODO:  aliases?

        # for   field   widgets   -- involving references
        # TODO: for Expression we could also construct IS_IN_SET or so..  (with distinct)

        if hasattr(self, 'type'):
            # f =  self.target_expression

            # def default_IS_IN_DB(*args, **kwargs ):
            #     return IS_EMPTY_OR(IS_IN_DB(*args, **kwargs))
            #
            # default_IS_IN_DB = kwargs.get('default_requires', default_IS_IN_DB)

            # if getattr(self, 'tablename', 'no_table') == 'no_table': # if orphan field
            if hasattr(self, 'tablename') and self.tablename == 'no_table': # if orphan field
                # we infer requires  from  FK
                if self.type.startswith('reference ') or self.type.startswith('list:reference '):
                    foreign_table = self.type.split()[1]
                    # self.target_expression = db[foreign_table]._id  # probably better no, as looses info
                    if not self.requires or self.requires == DEFAULT:
                        self.requires = IS_EMPTY_OR(IS_IN_DB(db, db[foreign_table], db[foreign_table]._format))
                        self.validator_overriden = True


            # TODO: after changing type to reference -- apply logic from previoius paragraph
            if self.type == 'id':  # override, as otherwise field is not shown in form
                self.type = 'reference %s' % self.tablename
                self._table = self.table = db[self.tablename] # maybe unnecessary
                if not self.requires or self.requires == DEFAULT:
                    self.requires = IS_EMPTY_OR( IS_IN_DB( db, self.table, self.table._format ) )
                    self.validator_overriden = True


    
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
        
def traverse_fields(items, flattened, field_decorator):  # works recursively
    """ is needed for SOLIDFORM, because it gets  *fields  matrix,    not flat list
    Recursivelly makes fields FormField (or so) instances and generates flattened list
    """
    result = []
    for nr, item in enumerate(items):

        if isinstance(item, (list, tuple)):  # if we get a list
            result.append(traverse_fields(item, flattened, field_decorator))  # apply recursively

        else:  # simple field

            #  make FormField instance if needed
            if not isinstance(item, field_decorator):
                item = field_decorator(item)  # apply with defaults

            # item.writable = True
            # item.readable = True
            result.append(item)
            # print( "DBG field name:", item.name )
            flattened.append(item)

    return result

class AnySQLFORM( object  ):
    """Works as proxy """
    def __init__(self, *fields,  **kwargs ):  #
        # SQLFORM.__init__(self, *fields, **kwargs )

        ####### prepair fields #############

        field_decorator = kwargs.pop('field_decorator', FormField)

        self.formfields = []
        self.structured_fields = traverse_fields( fields, self.formfields, field_decorator  )
        # self.formfields = [f if isinstance(f, FormField) else FormField(f) for f in fields ]

        # assertions
        try:
            self.check_duplicate_fields_by_attrs('name')
        except RuntimeError as e:
            raise RuntimeError("Form fields shouldn't have same names. \n %s" % e)

        # self.default_IS_IN_DB = kwargs.pop('default_IS_IN_DB', None)
        for f in self.formfields:
            self.set_default_validator(f)

        ######### generate form #############

        # factory could be SQLFORM.factory or SOLIDFORM.factory or so..
        form_factory= kwargs.pop('form_factory', SQLFORM.factory)
        self.table_name  = kwargs.pop('table_name',  DEFAULT_TABLE_NAME)

        kwargs.setdefault('formstyle', 'table2cols')


        self.__form = form_factory ( *self.structured_fields, table_name=self.table_name, **kwargs )
        # print "dbg form", self.__form
        
        self.db = current.db

    def set_default_validator(self, f):
        """to be overriden -- example in translations..."""
        # TODO maybe use  gluon.validators  _default_validators()
        pass



    def __getattr__( self, name ):
        # try:
        #     object.__getattribute__(self, name)
        # except AttributeError as e:

        if name in  self.__dict__:
            return  self.__dict__[ name ]
        else:
        # elif '__form' in self.__dict__:
            return getattr(self._AnySQLFORM__form, name)
            # return object.__getattribute__(self.__form, name)
        # else:
        #     raise AttributeError("Attribute %s  not found in  %s (__form)" % (name, self))

        # try:
        #     return getattr( self.__form, name )
        # except AttributeError as e:
        #     raise AttributeError("Attribute %s  not found in  %s" % (name, self) )

    def find_fields(self, **kwargs):
        def match_kwargs(f):  # helper
            for key, val in kwargs.items():
                if (
                    not hasattr(f, key)
                    or not( getattr(f, key) == val )
                    or not( str(getattr(f, key)) == str(val) )  # for instances of same Class, example, Expression
                    ):
                    return False
            return True
        return [f    for f in self.formfields     if match_kwargs(f)]


    def get_field(self, arg=None, **kwargs ):
        "finds field(s) according to their name / target_expression or other attr"
        if isinstance(arg, Expression):
            kwargs['target_expression'] = arg
        if isinstance(arg, str):
            kwargs['name'] = arg

        result = self.find_fields( **kwargs)

        if len(result)==1:
            return result[0]
        elif len(result) > 1 :
            raise RuntimeError("Duplicate  %s in fields %s" % (arg, result))
        # raise KeyError("FormField '%s' not found" % arg)

    def check_duplicate_fields_by_attrs(self, *attr_names):
        for f in self.formfields:
            attrs = {  aname: getattr(f, aname)  for aname in attr_names  }
            fields = self.find_fields( **attrs )
            if len(fields) > 1:
                raise RuntimeError("Duplicatesin fields %s  %s" % (map(str,fields), attrs))
        return True


    def get_value(self, field, vars=None):
        if not vars:
            # default to self.vars
            if not self.vars:
                # session=None, formname=None  ---  to prevent _formkey (and _formname), which later wouldn't let multiple times access via ajax
                self.process(session=None, formname=None, keepvalues=True, hideerror=True, dbio=False, onfailure=None)
                # self.process(keepvalues=True, hideerror=True, dbio=False)
            vars = self.vars
        # return current.request.vars.get( field.name, None ) 
        
        return vars[field.name] # could be request vars


    def vars_as_Row( self, vars=None ): # but types are not checked/converted
        """
        Nicely groups form input values according to their target tables
        FIXME:  if target (field/expr)  has several inputs, only the last's value is stored :/
        :param vars:
        :return: Row object
        """
        row = defaultdict( dict )
        
        # if vars is None:
            # vars = current.request.vars

        for f in self.formfields:
            # if f.name in vars:  # this would cause some of stuff missing..
                # value = vars[f.name]
                value = self.get_value( f , vars=vars)
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
           comparison
           name_extension

           query_function
           target_is_aggregate
        """
        
        FormField.__init__(self, field, **kwargs )  # overrides "construct_new_name" and inits 'comparison', 'name_extension'
        
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
                val  = getattr(self, key, None) # maybe we already had property (in field) # TODO: was  getattr(self, key, None)
            setattr(self, key, val)

    def init_comparison_and_name_extension(self, field, kwargs):
        # comparison
        if not hasattr(self, 'comparison'):
            self.comparison = find_out_attr('comparison', [ kwargs, field], defaultsNone=True)

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
            def query4filter_direct_val(expr, op, value): # BUG FIXME: throws "...unrecognized token:" when trying to execute query
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

        """  Moved to GrandRegister  DalView

        if multiple tables will be in context
        should get parameter "join_chains", which is matrix of tablenames, each row represents some logicat join chain for "left" argument


        # for data model/view construction -- would be reused in build_queries and  field validators IS_IN_DB
        # for key in 'left, join_chains, orderby, groupby, distinct'.split(', '):
        #     setattr(self, key, kwargs.pop(key, None) )
        #
        #
        # if self.join_chains and not self.left:
        #     self.left = []
        #     for jchain in self.join_chains:
        #         self.left.extend( build_join_chain( jchain ) )
        """
        # table_name = kwargs.pop('table_name', 'QuerySQLFORM'),
        kwargs.setdefault('table_name', 'QuerySQLFORM')
        # AnySQLFORM.__init__(self, *fields, field_decorator=SearchField,  **kwargs)
        super(QuerySQLFORM, self).__init__( *fields, field_decorator=SearchField,  **kwargs)
        # self.formfields = [f if isinstance(f, SearchField) else SearchField(f) for f in fields ]
        # assertions
        try:
            self.check_duplicate_fields_by_attrs('target_expression', 'comparison')
        except RuntimeError as e:
            raise RuntimeError("QueryForm fields shouldn't have same combination of expression and comparison. \n %s" % e)

    def set_default_validator(self, f):

        db = current.db  # todo: for Reactive form should be prefiltered dbset
        target = f.target_expression  # for brevity

        if f.type.startswith('reference '):
        # or f.type.startswith('list:reference '):
            foreign_table = f.type.split()[1]
            f.requires = IS_IN_DB(db, db[foreign_table], db[foreign_table]._format)

        elif f.type in ('string', 'text'):
            if isinstance(target, Field):
                if f.comparison == 'equals':
                    f.requires = IS_IN_DB(db, target)

                if f.comparison == 'contains':
                    # f.requires = IS_IN_DB(db, target)
                    # http://web2py.com/books/default/chapter/29/07/forms-and-validators#Autocomplete-widget
                    f.widget = SQLFORM.widgets.autocomplete(
                        current.request,
                        target
                        # , keyword='_autocomplete_%(tablename)s_%(fieldname)s__'+f.name # in case there would be 2 same targets
                        #, id_field=db.category.id
                    )


                # elif type(target) is Expression:
                #     table = target._table
                #     theset = db(table).select(target).column(target)
                #     f.requires = IS_IN_SET(theset)
        else:
            AnySQLFORM.set_default_validator(self, f)


    def build_queries(self, ignore_orphaned_fields=False):
        db = self.db
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
        

###########
# some helpers

def get_expressions_from_formfields( formfields, include_orphans=False ):
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


############
# class ReactiveSQLFORM():
    # def callback():
        # pass
