# -*- coding: utf-8 -*-
from gluon import current

from gluon.sqlhtml import represent
from gluon.storage import Storage
from pydal.objects import Field, Expression, Table

from helpers import represent_boolean, represent_datetime # action_button
from lib.w2ui import make_orderby, save_export, serialized_lists

import traceback

def w2ui_colname( field ):
    """"""
    
    if isinstance (field, Expression) and not isinstance (field, Field):
        result = ""
        if hasattr(field, 'tablename'):
            result = field.tablename+"."
        if hasattr(field, 'name'):
            result += field.name
        else:
            result += str(field)
    else:
        result =  str(field)     
    
    result = str(result).replace('.', ':')  # because w2ui interprets '.' more than needed
    result = result.replace("'", "\\'")   # escaping single quote, because w2ui generates some js code putting colnames in single quotes   
    return result

def w2ui_colname_decode( name ):
    return name.replace(':', '.').replace("\\'", "'")

def inject_attrs(obj, _override=False, **kw):
    for key, val in kw.items():
        if not hasattr(obj, key):
            setattr(obj, key, val)
        elif _override:
            setattr(obj, key, val)
        else:
            raise AttributeError("inject_attrs does'nt know what to do: " + str((obj, _override, key, val)) )
    return obj
    

def define_w2ui_columns( fields_4columns, custom=None ):
    """ custom -- dictionary of field:params
    """
        
    # default params
    columns = [
             {'field': w2ui_colname(f), 'caption': f.label, 'size': "100%", 'sortable': isinstance(f, (Field, Expression)), 'resizable': True}
             for f in fields_4columns 
         ]
         
    def get_col_params( field ):
        for params in columns:
            if params['field'] == w2ui_colname(field):
                return params

    # custom column parameters
    # w2ui in parameters stores field as name, but in "custom" dictionary we provide keys as Field -- foreasier call
    if custom:
        for field, params in custom.items():
            get_col_params( field ).update( params )
    
    return columns

def w2ui_grid_data(query, 
            fields_4columns ,  # list of :  Field, Expression or VirtualField 
            left=None, join=None, groupby=None, having=None, 
            after_select_before_render=None, # function with some extra requests or so... -- must return a dictionary!
            data_name=None, # used in has_permission(..)
            table_name=None,
            **kwargs
            ):
                
    """
    items in fields_4columns can have special attributes:
        - represent4export   #  overrides representation  for csv -- mapping colnames to functions
        - needed_data    # list of expressions needed to be fetched by select for Field.Virtual   
    """
                
    request = current.request
    db = current.db
    DBG = current.DBG   # FIXME
    if DBG(): 
        TOTAL_ROWS = '42' # "count(*) over()" # :)
    else:
        from helpers import TOTAL_ROWS

    MSG_NO_PERMISSION = current.MSG_NO_PERMISSION
    MSG_ACTION_FAILED = current.MSG_ACTION_FAILED


    auth = current.auth

    ctx = Storage()   # similar to "self" in object -- instead of "nonlocal"

    table_name = table_name or fields_4columns[0]._tablename       # with hope, that  fields_4columns[0] is not some Field.Virtual or Expression
    data_name = data_name or table_name or request.controller 

    # id_field is not shown, but kept in grid for edit/delete..
    id_field = db[table_name]._id 

    ctx.update(kwargs)

    status = 'success'
    
    cid = request.vars.cid
    cmd = request.vars.cmd
    
    def select_and_render_records(limitby=None):
        if not (DBG() or auth.has_permission('list', data_name)):
            return {'status': 'error', 'message': MSG_NO_PERMISSION}

        extra = serialized_lists(request.vars)  # sort, search
    
        if 'sort' in extra:
            # TODO FIXME : doesn't work for expressions 
            def find_expr_by_colname( colname ):
                for f in fields_4columns:
                    if type(f)==Expression:
                        if hasattr(f, 'name') and f.name == colname  \
                        or hasattr(f, 'label') and f.label == colname:
                            return str(f)
                        
            fields_mapping = { x['field']: find_expr_by_colname(x['field']) or w2ui_colname_decode(x['field'])  for x in extra['sort'] }
            
            orderby = make_orderby(db, extra['sort'], fields_mapping=fields_mapping)   
        else:
            orderby = None

        virtual_fields = [f for f in fields_4columns   if isinstance( f, Field.Virtual )]
        fields_4virtual= [d for f in virtual_fields for d  in f.needs_data    ] # extraxt  fields/expressions, needed to calculate virtual fields 
        
        fields_4select = [f for f in fields_4columns if isinstance( f, (Field, Expression) ) ] # ignores Field.Virtual's
        # if fields_4select == []: fields_4select = db[request.controller].ALL  # RISKY
        fields_4select += fields_4virtual
        # if not id_field in fields_4select:  fields_4select.append( id_field )

        ###############   SELECT  #################### 
        
        
        # sql = db(query)._select(  # dbg purposes
            # *(fields_4select+[TOTAL_ROWS]), 
 
            # orderby=orderby,
            # limitby=limitby,
            # #not mandatory
            # left=left,
            # join=join,
            # groupby=groupby
            # ,having=having
        # )       
        # print "DBG sql", sql
        
        ctx.rows = db(query).select(
            *(fields_4select+[id_field, TOTAL_ROWS]), 

            orderby=orderby,
            limitby=limitby,
            #not mandatory
            left=left,
            join=join,
            groupby=groupby
            ,having=having
        )
    
        if after_select_before_render:
            ctx.update( after_select_before_render(ctx.rows, cmd, ctx) )
        
        # RENDER
        records = []
        for row in ctx.rows:
            records.append( render_record(row) )

        return records
        
    def get_records(): 

        offset = int(request.vars.offset)
        limit = int(request.vars.limit)
        limitby = (offset, offset + limit)
        
        records = select_and_render_records(limitby=limitby)
        total = ctx.rows[0][TOTAL_ROWS] if ctx.rows else 0
        return {'status': status, 'total': total, 'records': records}

    def export_records():
        for field in fields_4columns:
            # if w2ui_colname(field) in represent4export:
                # field.represent = represent4export[ w2ui_colname(field) ]
            if hasattr(field, 'represent4export'):
                field.represent = getattr( field, 'represent4export' )
            elif field.type == 'boolean':
                field.represent = lambda val: represent_boolean(val, html=False)
            elif field.type == 'datetime':
                field.represent = lambda val: represent_datetime(val, html=False, empty='')
                # was sth like:    
            # db.auth_user.active.represent = lambda value: represent_boolean(value, html=False)
            # TODO: could do automatically for common types (boolean, datetime...)

        records = select_and_render_records()
        
        labels = {
            w2ui_colname(f): f.label    for f in fields_4columns 
        }
        
        # Filter just visible columns
        visible_columns = request.vars.getlist('columns[]')
        data = [[labels[c] for c in visible_columns]]  # TODO: arba ištrint record'us atminties optimizavimui
        for r in records:
            data.append([r[c] for c in visible_columns])

        save_export(cid, data)
        return {'status': status}

    def delete_records():
        if not (DBG() or auth.has_permission('delete', data_name)):
            return {'status': 'error', 'message': MSG_NO_PERMISSION}

        selected = request.vars.getlist('selected[]')
        try:
            for s in selected:
                # del db.auth_user[s]
                del db[table_name][s]
        except:
            db.rollback()
            return {'status': 'error', 'message': MSG_ACTION_FAILED}
        return {'status': status}

        
    def render_record(row, **kwargs):
        result = {}
        for field in fields_4columns:
            if isinstance( field, Field.Virtual ):
                rendered = field.f( row , ctx=ctx)
            elif isinstance( field, Field ):
                rendered = represent(
                                field,
                                row[field._tablename][field.name],
                                row[field._tablename] 
                            )  
            elif hasattr( field, 'represent' ): # for Expression with such attr 
            # elif isinstance( field, Expression ): # would throw error if no represent
                rendered = represent (field, row[field], row )  
            else:
                rendered = row['_extra'][field]    # TODOL FIXME: test it?? maybe in _extra?
        
            result[ w2ui_colname(field) ] = rendered
            
        result['recid'] = row[id_field] #  ex, db.auth_user.id
        
        return result
        
    
    try:  
    # if True:
        # dispatch by cmd
        if cmd=='get-records'   : return   get_records()
        if cmd=='export-records': return export_records()
        if cmd=='delete-records': return delete_records()

        return {'status': status}  # default
    except Exception as e:
        return   { 'status':"error",  'message':str(e)+traceback.format_exc() }
    

 
