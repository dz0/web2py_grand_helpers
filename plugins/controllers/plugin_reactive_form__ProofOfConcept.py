# -*- coding: utf-8 -*-
from plugin_search_form.search_form import SearchField, SearchForm
from plugin_joins_builder.joins_builder import build_joins_chain , get_referenced_table # uses another grand plugin
from  gluon.serializers import json

"""
TEST REACTIVE FORM
"""

def html_id(field, table_name='no_table'):
    "gets html_id by field"
    # result = str(field).replace('<no table>', 'no_table').replace('.', '_') 
    result = table_name+"_"+field.name # factory overrides original tablenames :/
    result = result.replace('<no table>', 'no_table').replace('.', '_') 
    return result

def keepvalues_workaround( fields ):
    # instead of keepvalues=True
    for f in fields:
        #if f.name in request.vars:
            f.default = request.vars[f.name]

def fullname_4htlm_form(field):
    return str(field).replace(".", "__")
    
def make_search_field(f):
    """to have tablename info in form input field name"""
    return f.clone(name=fullname_4htlm_form(f))  

def get_str_expr_from_fullname4htlm( fullname ):
    return fullname.replace("__", ".")

def get_field_from_fullname4htlm( fullname ):
    tname, fname = fullname.split("__")
    return db[tname][fname]
    

def get_field_by_html_id(fields, htmlid, table_name='no_table'):
    return [t for t in fields if html_id(t, table_name) == htmlid][0]

def ajax_triggers_js(triggers, field_names=[], table_name='no_table', **update_url_kwargs):   
    """Generates js code, which will prepaire to call ajax updates
    triggers -- dictionary of field dependancy { the_triggering_field: [dependant fields] } 
    field_names -- pased to web2py "ajax" call
    update_url_kwargs   can indicate other  app/controller/function than  currently called
    """ 

    # will inject onchange triggers (for ajax calls)
    js = ""
    for trigger in triggers:
        trigger =  trigger
        trigger_htmlid = html_id(trigger, table_name) 
        update_url = URL(vars={'trigger':html_id(trigger, table_name) }, extension=None, **update_url_kwargs)
        
        if trigger.widget == SQLFORM.widgets.radio.widget:
            # elements_set = "\n   jQuery('form #%s.web2py_radiowidget input[name=\"%s\"]:radio')" % ( html_id(trigger), trigger.name ) 
            elements_set = "\n   jQuery('form #%s.web2py_radiowidget input:radio')" % trigger_htmlid
        else:
            elements_set = "\n   jQuery('form #%s.generic-widget')" % trigger_htmlid
       
        # js += "alert('trigger will be: #%s');" % trigger_htmlid
        js += (
              elements_set
              # since jquery 1.7 on/off is recomended instead of bind/unbind (as they are deprecated since jquery 3)
              # + ".off( 'change.reactive_form')" 
              + ".unbind( 'change.reactive_form')"    # remove previous triggers (otherwise they will trigger too much times)
              # + ".on('change.reactive_form', function() {      ajax('%s', %s, ':eval')         }); " % ( update_url, field_names)      
              + ".bind('change.reactive_form', function() {      ajax('%s', %s, ':eval')         }); " % ( update_url, field_names)      
              )
            
    js = """\n var plugin_reactive_form_inject_ajax_update_triggers=function(){  \n      %s   \n};  
            \n plugin_reactive_form_inject_ajax_update_triggers(); \n""" % js

    return js

def ajax_response_js_NG(updatables, form, table_name='no_table'):  # TODO table_name can be derived form.table_name
        table_name = table_name or form.table._tablename
        js = ""
        for field in updatables:  # what we need to update
            # target_id = html_id( make_search_field(field), table_name)
            target_id = html_id( field, table_name)
            widget = form.custom.widget[field.name]
            # js += "\n   alert('updating  #%s '); " % (target_id)
            js += "\n   jQuery('form #%s.generic-widget').replaceWith(%s); " % (target_id, json(widget) )
        js += "\n plugin_reactive_form_inject_ajax_update_triggers(); \n"
        return js
 

def ajax_response_js(triggers, form, table_name=None):  # TODO table_name can be derived form.table_name
    """TODO: "triggers" are needed just to get "updatables"
    """
    if request.vars.trigger:  # used for ajax response
        trigger = get_field_by_html_id( triggers, request.vars.trigger , table_name)
        updatables = triggers[trigger]
        
        return ajax_response_js_NG(updatables, form, table_name)
           
        # for dbg purposes
        # result = [ form.custom.widget[field.name] for field in updatables ]
        # return UL(result)



def tester(query, form, selected_fields, **kwargs):

     
    main_table = selected_fields[0].table

    if query==True: query = main_table.id > 0
    
    sql = db(query)._select( *selected_fields, **kwargs )    
    print( "DBG SQL: ", sql )
    
    # data =  db(search.query).select( *selected_fields, **kwargs )  
    data = SQLFORM.grid(query, fields=selected_fields, **kwargs )  # can toggle
    # data = get_data_with_virtual()
    
    # data.colnames = [". %s ." % f.replace('.', '\n') for f in data.colnames ] 
    
    # menu4tests()
    return dict( z_data = data, 
                # sql = XML(str(db._lastsql[0]).replace('HAVING', "<br>HAVING").replace('WHERE', "<br>WHERE").replace('AND', "<br>AND").replace('LEFT JOIN', '<BR/>LEFT JOIN ')), 
                sql = XML(str(sql).replace('HAVING', "<br>HAVING").replace('WHERE', "<br>WHERE").replace('AND', "<br>AND").replace('LEFT JOIN', '<BR/>LEFT JOIN ')), 
                form=form,  
                # extra=response.tool, 
                # query=search.query.as_dict(flat=True)   
                query=XML(str(query).replace('AND', "<br>AND"))
                )
       
def test_auth_model_oldschool_compare_equals(): # TODO

    # db.auth_user.id.requires = IS_IN_DB(db, db.auth_user.id, '%(first_name)s')
    db.auth_user.first_name.requires = IS_IN_DB(db, db.auth_user.first_name)
    db.auth_membership.group_id.widget =  SQLFORM.widgets.radio.widget
    db.auth_membership._format =  "bla %(group_id)s"
    db.auth_permission.table_name.requires =  IS_IN_DB(db,  db.auth_permission.table_name)
    # db.auth_permission.table_name.widget =  SQLFORM.widgets.radio.widget

    fields = [        
         # db.auth_user.id,
         db.auth_user.first_name,
         db.auth_membership.group_id,
         # db.auth_group.role, 
         db.auth_permission.table_name #,   db.auth_permission.name
    ]
    
    triggers = {
        db.auth_user.first_name: [db.auth_membership.group_id, db.auth_permission.table_name ],
    }
    
    # convert triggers to search fields
    triggers = { make_search_field(trigger) : map(make_search_field, updatables)  
                 for trigger, updatables in triggers.items() }
    
    left = build_joins_chain( 'auth_user', db.auth_membership, 'auth_group', db.auth_permission.group_id  )

    def construct_query(sfields):
        queries = []
        for sf in sfields:
            if request.vars[ sf.name]:
                queries.append(  get_field_from_fullname4htlm(sf.name)  == request.vars[sf.name] )
                # queries[sf.name] =   get_field_from_fullname4htlm(sf.name)  == request.vars[sf.name] 
        if queries:
            return reduce(lambda a, b: (a & b), queries) 
        else:
            return True
                
    
    if request.vars.trigger:
        def ajax_response():
            trigger = get_field_by_html_id( triggers, request.vars.trigger , table_name="jurgio")
            session.trigger= html_id(trigger)
            updatables = triggers[trigger]
            keepvalues_workaround ( updatables )
            
            query = construct_query([trigger]) #  based ONLY on current trigger
            
            def select_subsets(query, updatables):
                # SUBSETS by QUERY
                print "\nDBG QUERY", query
                for sf in updatables:
                    f = get_field_from_fullname4htlm(sf.name) 
                    # sql = db(query)._select( f, left=left, distinct=True )
                    # rows =  db(query).select( f , left=left, distinct=True )
                    # print "DBG rows:", rows
                    
                    # print "DBG db[f._tablename]._id:", db[f._tablename]._id
                    if db[f._tablename]._id is f: # if field is private ID
                        # print "DBG   %s   is PK of   %s" % ( f, db[f._tablename] )
                        label = db[f._tablename]._format
                    elif get_referenced_table(f): # if field is foreign ID #TODO https://groups.google.com/forum/#!topic/web2py/4QmDyCh1i2A
                        # print "DBG   %s   is FK of   %s" % ( f, get_referenced_table(f))
                        label = db[get_referenced_table(f)]._format
                        f = db[get_referenced_table(f)]._id  # override f with PK of foreign table
                    else:
                        label = None
                    
                    if query == True: # nothing selected -- default fake query
                        db_set = db()  # select everything
                        left_local = None
                    else:
                        db_set = db(query)
                        left_local = left
                        
                    sf.requires = IS_IN_DB( db_set, f , label=label,  left=left_local, distinct=True )
            
            select_subsets( query, updatables )
            form = SQLFORM.factory( *updatables, table_name="jurgio") # construct form with ONLY fields to be updated 
            
            js = ajax_response_js_NG(updatables, form,  table_name="jurgio")
            session.ajax_response_js = js
            return js
        return ajax_response()
        
    else:
        # define search form fields
        sfields = map(make_search_field, fields)        
        query = construct_query(sfields)
        keepvalues_workaround( sfields )
        form = SQLFORM.factory( *sfields, table_name="jurgio")
        # form.process(keepvalues= True)
        js4triggers=SCRIPT(ajax_triggers_js(triggers, field_names=[sf.name for sf in sfields], table_name="jurgio"))
        # print "DBG js4triggers
        form = CAT(form, js4triggers)
        
        return tester(query, form, fields, left=left)
    

                
def test_auth_model_pluginSEARCH(): # TODO
    def ID_search_field(table_name):
         return SearchField( Field(table_name, 'integer', 
                     requires=IS_IN_DB(db, table_name+'.id' ,  db[table_name]._format)), 
                     target_expression = db[table_name].id
                    )        
        
    fields = [        
         db.auth_user.first_name,    db.auth_user.email, 
         db.auth_membership.id,
         db.auth_group.role, 
         db.auth_permission.name,     db.auth_permission.table_name
    ]
    
    left_join = build_joins_chain( 'auth_user', db.auth_membership, 'auth_group', db.auth_permission.group_id  )

    # FORM
    user_id_search = ID_search_field('auth_user')
    membership_id_search = ID_search_field('auth_membership')
    membership_id_search.field.widget = SQLFORM.widgets.radio.widget

    # form requires
    def mebership_subset(): 
        try:
            rows = db(request.vars[user_id_search.target_expression.name]>0).select( db.auth_membership.id, left=left_join, distinct=True)
        except Exception as e:
            response.flash = str(e)
            rows = db(db.auth_membership.id >0).select(db.auth_membership.id, distinct=True)
            
        return [r[db.auth_membership.id] for r in rows if r[db.auth_membership.id]]
        
    membership_id_search.field.requires = IS_EMPTY_OR(IS_IN_SET( mebership_subset()  ))

    search = SearchForm(  
                # db.auth_user.first_name,
                user_id_search,
                membership_id_search
                )
    form = search.form
    
        
    
    def get_triggers_by_fields():
        # triggers_by_table = {
            # db.auth_user: [db.auth_membership, db.auth_group, db.auth_permission ],
            # db.auth_membership: [ db.auth_group, db.auth_permission ],
            # db.auth_group: [ db.auth_permission ]
        # }
        triggers = {user_id_search.field: [membership_id_search.field]}
        return triggers

    triggers = get_triggers_by_fields()

    
    # update required sets for fields before constructing form 
    if request.vars.trigger:
        
        trigger = get_field_by_html_id( triggers, request.vars.trigger )
        session.trigger= html_id(trigger)
        updatables = triggers[trigger]
        if trigger == user_id_search.field:  # HARDCODED auth_user.id
            
            if request.vars[ html_id(trigger)]:
                
                # form =  SQLFORM.factory( *updatables ) # construct form with ONLY fields to be updated 
                js = ajax_response_js(triggers, form)
                session.ajax_response_js = js
                return js
                # works http://localhost:8001/plain_app/plugin_reactive_form/test_auth_model?trigger=Search_form__no_table__auth_user__equal&Search_form__no_table__auth_user__equal=2
                
    # js = ajax_triggers_js( triggers , field_names=[html_id(f.field) for f in search.fields] ) 
    js = ajax_triggers_js( triggers , field_names=[ ] ) 
    session.ajax_triggers_js = js
    form = CAT( form, "SCRIPT",  SCRIPT(js) )

    # return dict(
        # form = form, 
    # )
    
    return tester(  search.query, 
                    form = form,
                    selected_fields=fields ,
                    left = left_join,
                    # distinct = True
                    # groupby = fields
   )     
     
    
def test_data_dict():

    data = { 
        'a': 'apple ant argument'.split(),
        'b': 'boy'.split(),
        'c': 'chrome cat'.split(),
    }

    def get_all_children (data_dict):
        return [child    for child_list in data_dict.values() for child in child_list ]

     
    subchilds = { key: [key+i for i in (" 1", " 2") ] for key in get_all_children( data ) }
    
        
    parent = Field('parent', requires=IS_IN_SET( sorted(data.keys()) ) )
    parent.children = data
    bla = Field('bla')
    child = Field('child', requires=IS_IN_SET( sorted( subchilds.keys() ) ), widget=SQLFORM.widgets.radio.widget )
    child.parent_field = parent
    child.children = subchilds
    subchild = Field('subchild', requires=IS_IN_SET( sorted( get_all_children( subchilds )) ), widget=SQLFORM.widgets.radio.widget )
    subchild.parent_field = child
    subchild.children = {key:[] for key in get_all_children( child.children )}
    
    a_data = { p:{c:subchilds[c] for c in data[p]}  for p in data}
     
    fields = [ parent, child, subchild ]
    # def get_field_by_name(name):
        # for f in fields: 
            # if f == name:
                # return f
    
    #  hack instead of keepvalues=True
    for f in fields:
        #if f.name in request.vars:
            f.default = request.vars[f.name]

    def filter_dict( data_dict, keys ):
        return { key:data_dict[key] for  key in keys }

    
        
    ######### react stuff #########
    #
    #  rule of thumb:  dependant fields should be mentioned later than their triggers
    #
    ##############################
    
    triggers = {
        parent : [ child, subchild ],
        child : [ subchild],
    }

    errors = {}
    
    # update required sets for fields before constructing form 
    if request.vars.trigger:
        
        # trigger = [t for t in triggers if html_id(t) == request.vars.trigger][0] # clumsy way to get trigger field from fieldname
        trigger = get_field_by_html_id( triggers, request.vars.trigger )
        updatables = triggers[trigger]
        if request.vars[trigger.name]:
            
            
            # data mangling...
            trigger.children = filter_dict(  trigger.children, [ request.vars[trigger.name] ] )  # subselect
            for field in updatables:
                try:
                    parent = field.parent_field
                    field.children =  filter_dict( field.children , get_all_children( parent.children )) # subselect
                    field.requires = IS_IN_SET(  sorted( field.children.keys()) )
                except KeyError as e:
                    errors[ field.name ] = str(e)
    
            # construct form with ONLY fields to be updated 
            form =  SQLFORM.factory( *updatables )
            return ajax_response_js(triggers, form)
        
        
    form = SQLFORM.factory( 
        *fields 
        # ,keepvalues = True
    )
    form.errors.update( errors  )
    
    js = ajax_triggers_js( triggers , field_names=[f.name for f in fields] ) 
    form = CAT( form, SCRIPT(js) )

    return dict(
        errors = errors,
        duomenys = a_data,
        form = form, 
    )
