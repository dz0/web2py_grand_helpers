# -*- coding: utf-8 -*-
from gluon import current

from gluon.sqlhtml import represent
from gluon.storage import Storage
from pydal.objects import Field, Expression, Table

from helpers import (action_button, expandable_form_fields, expandable_section, FORM_SEPARATOR, random_password, represent_boolean)
from lib.w2ui import make_orderby, save_export, serialized_lists


def w2ui_colname( field ):
    """because w2ui interprets '.' more than needed"""
    return str(field).replace('.', ':')
    # return str(field)

def w2ui_colname_decode( field ):
    """because w2ui interprets '.' more than needed"""
    return str(field).replace(':', '.')
    
# def unpack_w2ui_colname( name ):
    # if ':' in name:
        # table_name, field_name = name.split(':')
        # return table_name, field_name 
     
# @auth.requires_signature()
def w2ui_grid(query, 
            fields_4columns ,  # list of :  Field, Expression or VirtualField 
            fields_4virtual=[],  # extra fields, needed to calculate virtual fields 
            left=None, join=None, groupby=None, having=None, 
            represent4export={},    #  overrides representation  for csv -- mapping colnames to functions
            after_select_before_render=None, # maybe some extra requests... -- must return a dictionary!
            data_name=None, # used in has_permission(..)
            table_name=None,
            **kwargs
            ):
                
    request = current.request
    db = current.db
    DBG = current.DBG
    if DBG(): 
        MSG_NO_PERMISSION = ('Insufficient privileges')
        TOTAL_ROWS = '42' # "count(*)" # :)

    auth = current.auth

    ctx = Storage()   # similar to "self" in object -- instead of "nonlocal"

    table_name = table_name or fields_4columns[0]._tablename 
    data_name = data_name or table_name or request.controller 
  
    ctx.update(kwargs)

    status = 'success'
    
    cid = request.vars.cid
    cmd = request.vars.cmd
    
    def select_and_render_records(limitby=None):
        if not (DBG() or auth.has_permission('list', data_name)):
            return {'status': 'error', 'message': MSG_NO_PERMISSION}

        extra = serialized_lists(request.vars)  # sort, search
    
        if 'sort' in extra:
            fields_mapping = { x['field']:w2ui_colname_decode(x['field'])  for x in extra['sort'] }
            orderby = make_orderby(db, extra['sort'], fields_mapping=fields_mapping)
        else:
            orderby = None


        fields_4select = [f for f in fields_4columns if isinstance( f, (Field, Expression) ) ] # ignores Field.Virtual's
        fields_4select += fields_4virtual
        # if fields_4select == []: fields_4select = db[request.controller].ALL  # RISKY

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
            *(fields_4select+[TOTAL_ROWS]), 

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
            if w2ui_colname(field) in represent4export:
                field.represent = represent4export[ w2ui_colname(field) ]
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
            elif isinstance( field, Field  ):
                rendered = represent(
                                field,
                                row[field._tablename][field.name],
                                row[field._tablename] 
                            )  
            else:  # simple Expression or so..
                rendered = row['_extra'][field]    # ?? maybe in _extra?
        
            result[ w2ui_colname(field) ] = rendered
            
        id_field = fields_4columns[0]   # if not virtual
        if str(id_field) != str(id_field.table._id): raise ValueError("not ID field") # TODO
        result['recid'] = row[id_field] # should be ID! ex, db.auth_user.id
        
        return result
        
      
    # dispatch by cmd
    if cmd=='get-records'   : return   get_records()
    if cmd=='export-records': return export_records()
    if cmd=='delete-records': return delete_records()

    return {'status': status}  # default

