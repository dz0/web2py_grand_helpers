# -*- coding: utf-8 -*-
# from plugin_search_form.search_form import SearchField, SearchForm
# from plugin_joins_builder.joins_builder import build_joins_chain  # uses another grand plugin
from  gluon.serializers import json

"""
TEST SEARCH FILTERS QUERY from FORM
"""

def html_id(field):
    "gets html_id by field"
    result = str(field).replace('<no table>', 'no_table').replace('.', '_') 
    return result

def test_data_dict():

    data = { 
        'a': 'apple ant argument'.split(),
        'b': 'boy'.split(),
        'c': 'chrome cat'.split(),
    }
     
    subchilds = dict( 
            apple='Antaninis Alyvinis'.split(),
            ant='black red brown'.split(),
            argument='True False'.split(),
            boy='Jonukas Petriukas Onukas'.split(),
            chrome='browser'.split(),
            cat='Kicė Micė'.split()
    )
    
    def get_all_children (data_dict):
        return [child    for child_list in data_dict.values() for child in child_list ]
        
    parent = Field('parent', requires=IS_IN_SET( sorted(data.keys()) ) )
    parent.children = data
    bla = Field('bla')
    child = Field('child', requires=IS_IN_SET( subchilds.keys() ), widget=SQLFORM.widgets.radio.widget )
    child.parent_field = parent
    child.children = subchilds
    subchild = Field('subchild', requires=IS_IN_SET( get_all_children( subchilds ) ), widget=SQLFORM.widgets.radio.widget )
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
        if f.name in request.vars:
            f.default = request.vars[f.name]
    
        
    ######### react stuff #########
    #
    #  rule of thumb:  dependant fields should be mentioned later than their triggers
    #
    ##############################
    
    # react_depends_on = dict(
        # child = [parent],
        # subchild = [parent, child],
    # )
    
    triggers = {
        parent : [ child, subchild ],
        child : [ subchild],
    }

    # the reaction to selected value in "parent" field
    # if request.vars.parent:
        # child.requires = IS_IN_SET( data[request.vars.parent] )

    def filter_dict( data_dict, keys ):
        return { key:data_dict[key] for  key in keys }

    errors = {}
    
    # for trigger, updatables in triggers.items():
    if request.vars.trigger:
        trigger = [t for t in triggers if html_id(t) == request.vars.trigger][0] # clumsy way to get trigger field from fieldname
        updatables = triggers[trigger]
        if request.vars[trigger.name]:
            trigger.children = filter_dict(  trigger.children, [ request.vars[trigger.name] ] )  # subselect
            for field in updatables:
                try:
                    parent = field.parent_field
                    # if parent != trigger:
                        # all_parents = get_all_children ( parent.parent_field.children )
                        # parent.children = filter_dict( parent.children, all_parents )
                        
                    field.children =  filter_dict( field.children , get_all_children( parent.children )) # subselect
                    field.requires = IS_IN_SET(  field.children.keys() )
                except KeyError as e:
                    errors[ field.name ] = str(e)
    
        
        
    form = SQLFORM.factory( 
        *fields 
        # ,keepvalues = True
    )
    form.errors.update( errors  )
    
    
    # returned via ajax
    # update_url = URL(vars={'trigger':'parent' }, extension=None)
    update_url_asjs = URL(vars={'trigger':html_id(parent),  'asjs':'true' }, extension=None)
    ajax_target = "ajax_target" # 'no_table_child__row'
    field_names = [f.name for f in fields]

    if request.vars.trigger:
        if errors:
            return errors
        # updated_widgets  = [form.custom.widget[name] for name in request.vars.getlist('update_widgets') ]
        # return dict( updated_widgets=updated_widgets )
        # return form.custom.widget.child
        result = [ form.custom.widget[field.name] for field in updatables ]
        if request.vars.asjs:
            js = ""
            for field in updatables:
                target_id = html_id(field)
                widget = form.custom.widget[field.name]
                js += "\n   jQuery('form #%s.generic-widget').html(%s); " % (target_id, json(widget) )
                js += "\n inject_ajax_update_triggers(); \n"
            return js
            
        else:
            return UL(result)

   
    # inject onchange triggers
    js = ""
    for trigger in triggers:
        update_url = URL(vars={'trigger':html_id(trigger),  'asjs':'true' }, extension=None)
        
        if trigger.widget == SQLFORM.widgets.radio.widget:
            js += "\n\n //%s  radiobuttons" % trigger
            js += """\n   jQuery('form #%s.web2py_radiowidget input[name="%s"]:radio').change(function() {
                          ajax('%s', %s, ':eval') 
                        }); """  % (html_id(trigger), trigger.name, update_url, field_names)             
        
        else:
            
            js += """\n   jQuery('form #%s.generic-widget').change(function() {
                          ajax('%s', %s, ':eval') 
                        });"""  % (html_id(trigger), update_url, field_names) 
    js = "\n var inject_ajax_update_triggers=function(){  \n      %s   \n};\n\n  inject_ajax_update_triggers(); \n" %js
    form = CAT( form, SCRIPT(js) )


    return dict(
        errors = errors,
        duomenys = a_data,
        form = form, 
        ajax_rez = DIV(_id="ajax_target"),
        # manual_trigger_return_widgets = A('reload as widgets (separate target) (based on Parent)',   _href="javascript:ajax('%s', %s, '%#s') " % (update_url, field_names, ajax_target) ),
        manual_trigger_return_js = A('reload as JavaScript (inplace) (based on Parent)',   _href="javascript:ajax('%s', %s, '%#s') " % (update_url_asjs, field_names, ":eval") ),
        # manual_trigger_newWin = A('child widget in new window', _href=update_url, _target="test" )
    )
