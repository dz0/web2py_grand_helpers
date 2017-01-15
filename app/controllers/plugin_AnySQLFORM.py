# -*- coding: utf-8 -*-

from gluon import *
# from gluon.html import xmlescape
from gluon.sqlhtml import SQLFORM
from pydal.objects import Field, Row, Expression
from collections import defaultdict

from pydal._globals import DEFAULT

DEFAULT_TABLE_NAME = 'AnySQLFORM'

def test():
    
    user_full_name = db.auth_user.first_name + db.auth_user.last_name
    
    # db.auth_permission._format = "%(name)s %(table_name)s (%(id)s)"
    
    fields = [
        db.auth_user.first_name, 
        db.auth_user.last_name, 
        
        db.auth_user.id,
        
        db.auth_permission.table_name,
        db.auth_permission.id,
        
        Field( 'somefield' ),
        Field('user_id', 'reference auth_user', requires=IS_IN_DB(db,db.auth_user.id, '%(first_name)s %(last_name)s')),
        FormField(user_full_name, name='full_name'),
    ]
    
    print "DBG fields:", fields

    form = AnySQLFORM( *fields )
    # form = SQLFORM.factory( *fields )

    form.process()
    data = form.vars_as_Row()
        
    return dict(
            form=form, 
            # data = repr(data),
            data = data,
            vars=form.vars,
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
                 
    def __init__(self, field, **kwargs):
        """ field is of type Field or Expression
        """
 
        # populate based on   default_field_attrs  and  kwargs
        field_attrs = {}
        for attr in default_field_attrs:
            parent_attr = default_field_attrs[attr]
            if hasattr(field, attr):  # Field should have, but Expression would miss some
                parent_attr = getattr(field, attr)
            field_attrs[attr] = kwargs.get( attr, parent_attr ) # kwargs or field args
        if hasattr(field, '_rname'):
            field_attrs['rname'] = field._rname
        if hasattr(field, 'name'):
            new_name = field.name
        if 'name' in kwargs:
            new_name = kwargs['name']
        
        # call Super init
        Field.__init__(self, fieldname=new_name, **field_attrs)
               
        # self.__dict__ = field.__dict__.copy() no need
        self.target_expression = field  # we leave direct connection to the field -- for data to be compared/inserted
        
        # update .name for Field to include tablename
        
        if type(field) == Field:  
            if not hasattr(field, 'tablename'):
                field.tablename = 'no_table'
            self.name = field.tablename+"__"+field.name 
        
        self.__dict__.update( kwargs )
        
        if field.type == 'id':  # override, as otherwise field is not shown in form
            self.type = 'integer'
            self.requires = IS_IN_DB( db, field.table, label=field.table._format ) 
         
        # for search forms we would need comparis
        self.comparison = kwargs.get('comparison')
        if self.comparison is not None:
            self.name += "__"+self.comparison_name()
        
    def comparison_name(self):
        return {None:None, '=':'equals'}[self.comparison] 
            
    def construct_query(self, val):
        if self.comparison == '=': 
            return self.target_expression == val
        else:
            return self.query_function(val)

class AnySQLFORM( ):  
    """Works as proxy """
    def __init__(self, *fields,  **kwargs ):  #
        # SQLFORM.__init__(self, *fields, **kwargs )
        self.fields = fields
        self.formfields = [f if isinstance(f, FormField) else FormField(f) for f in fields ]
        
        # factory could be SQLFORM.factory or SOLIDFORM.factory or so..
        factory_class= kwargs.get('factory_class', SQLFORM)
        self.table_name  = kwargs.get('table_name',  DEFAULT_TABLE_NAME)
        self.__form = factory_class.factory ( *self.formfields, table_name=self.table_name, **kwargs )
    
    def __getattr__( self, name ):
        if name in  self.__dict__:
            return  self.__dict__[ name ]
        else:
            return getattr( self.__form, name )      

    def get_field( arg ):
        for ff in self.formfields:
            if isinstance(arg, Expression):
                if ff.target_expression == arg:
                    return ff
            elif isinstance(arg, str):
                if ff.name == arg:
                    return ff
        raise KeyError("FormField '%s' not found" % arg)
        
    def vars_as_Row( self, vars=None ): # but types are not checked/converted
        row = defaultdict( dict )
        
        if vars is None:
            vars = current.request.vars

        
        for ff in self.formfields:
            # if ff.name in vars:  # this would cause some of stuff missing..
                value = vars[ff.name]
                expr = ff.target_expression  # todo -- what if it is missing?
                if type(expr) is Field:
                    row[expr.tablename][expr.name] = value
                elif isinstance(expr, Expression): # not Field
                    row['_extra'][str(expr)] = value
        
        return Row( **row ) 
        

