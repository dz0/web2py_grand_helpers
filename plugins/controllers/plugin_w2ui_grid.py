# -*- coding: utf-8 -*-
from plugin_search_form.search_form import SearchField, SearchForm
from plugin_joins_builder.joins_builder import build_joins_chain 
from plugin_w2ui_grid.w2ui_grid import w2ui_grid_data  , w2ui_colname, w2ui_colname_decode
from plugin_w2ui_grid.w2ui_grid import inject_attrs


from gluon.sqlhtml import represent
from gluon.storage import Storage
from pydal.objects import Field, Expression, Table

from searching import search_form
from lib.w2ui import make_orderby, save_export, serialized_lists
from applications.app.modules.searching import search_form


"""
TESTS for "w2ui_grid"  
"""

def populate_users():
    from gluon.contrib.populate import populate
    populate(db['auth_user'],5)  ; db.commit()

def add_user():
    return "test..."
    
def test():  
    
    cid = request.function 
    data_name = 'user' # for checking permissions, etc..

    # SEARCH FORM
    search = SearchForm( 
        [ SearchField( db.auth_user.email ), SearchField( db.auth_user.first_name ) ],
        formstyle='table3cols',
        form_factory = lambda *a, **kw:  search_form(cid, *a, **kw), # pass cid to form facotry
        
        # w2ui
        _name = '{0}__form'.format(cid),
        _action = 'javascript:w2ui__grid("{0}");'.format(cid)
    )

    # GRID COLUMNS MODEL -- fields_4columns

    # Virtual Field
    reversed_name =  inject_attrs(
                        # db.auth_user.reversed_name =  Field.Virtual('reversed_name',  
                        Field.Virtual('reversed_name',  # it can be anonymous, but for SQLFORM.grid it requires table_name
                                lambda row, ctx=None: row.auth_user.first_name[::-1]
                        ),
                        needs_data = [db.auth_user.first_name ], 
                        #joins = None, left = None
                      ) 
                               
    
    # Expressions 
    # oversimplified Expression -- just a field
    name = inject_attrs( db.auth_user.first_name, label="field as expression", _override=True) 

    # robust expr
    full_name = db.auth_user.first_name+" "+db.auth_user.last_name # Expression
    full_name.name = "full_name" # or could use inject_attrs 
    full_name.label = "Full name" # or could use inject_attrs 
    full_name.represent = lambda val, row: ("Mrs. " if row[db.auth_user.first_name].endswith('a') else "Mr. ") +val  # demo of represent injection
    #full_name.join = None 
    
    fields_4columns=[   
                        db.auth_user.id,
                        db.auth_user.last_name,
                        reversed_name,
                        name,
                        full_name,
                        inject_attrs( db.auth_user.email, represent4export=lambda val: val.upper() ),
                    ] 

    def whole_page():
        response.view = 'plugin_w2ui_grid/w2ui_grid.html'
        
        # some workarounds for grand core stuff
        response.subtitle  = "test  w2ui_grid"
        response.menu = []
        
        return dict(        
            cid = cid,
            form = search.form,
            w2grid_columns =  [
                 {'field': w2ui_colname(f), 'caption': f.label, 'size': "100%", 'sortable': isinstance(f, (Field, Expression)), 'resizable': True}
                 for f in fields_4columns 
             ],
            grid_function = request.function,       # or specify other...     
            data_name = data_name , # request.controller could be default       
            context = request.controller +'_'+ data_name,

            w2grid_sort = [  {'field': w2ui_colname(db.auth_user.email), 'direction': "asc"} ]
        )
     
    # @auth.requires_signature()
    def grid_data():  # uses search.query  and   fields_4columns 
        
        # optional
        def after_select_before_render(rows, cmd=None, ctx=None):
            return {}  # return new stuff for context  -- might change
                     
        grid_kwargs_from_search = { key: search.get(key) for key in 'left join groupby having'.split() }
        
        return w2ui_grid_data(
                        search.query,   
                        fields_4columns=fields_4columns, 
                        after_select_before_render=after_select_before_render,
                        data_name=data_name, 
                        **grid_kwargs_from_search 
                        )
                  
    if request.vars._grid == 'True':  
        return grid_data()
    else:
        return whole_page()



# @auth.requires_permission('list', 'user')
def testgrand_users():
    from plugin_search_form.search_form import SearchField, SearchForm
    from plugin_joins_builder.joins_builder import build_joins_chain  # uses another grand plugin

    cid = request.function 
    data_name = 'user' # for checking permissions, etc..

    def my_grand_search_form(*fields, **kwargs):
        from applications.app.modules.searching import search_form as grand_search_form
        return grand_search_form(cid, *fields, **kwargs)
            

    search = SearchForm(  # jurgio plugin'as
        [ SearchField( db.auth_user.email ), SearchField( db.auth_user.first_name ) ],
        formstyle='table3cols',
        form_factory = my_grand_search_form,
        
        # w2ui
        _name = '{0}__form'.format(cid),
        _action = 'javascript:w2ui__grid("{0}");'.format(cid)

    )
    
    # def update_records_with_virtual_info(rows, records, cmd ):
        # for row, rec in zip(rows, records ):
            # row['virtual_color'] = "BLAH " + row.color
    
    def after_select_before_render(rows, cmd=None, ctx=None):
        
        return {}  # return new objects for context
        
    
        
    # FIELDS 
    # fields_4virtual = []  -->> needs_data

    db.auth_user.reversed_name =  Field.Virtual('reversed_name', 
                                                lambda row, ctx=None: row.auth_user.first_name[::-1]
#                                                 , table_name = "auth_user"  # not really necessary

                                                );
    # fields_4virtual.extend([db.auth_user.first_name ])

    db.auth_user.reversed_name  .needs_data =  [db.auth_user.first_name ]

    fields_4columns=[   
                        db.auth_user.id,
                        # db.auth_user.username,
                        db.auth_user.last_name,
                        db.auth_user.reversed_name,
                        db.auth_user.email ,
                        # db.auth_user.active,
                        # db.auth_user.type  ,
                        # db.auth_user.data_level ,
                    ] 
    
                
    """
    select_fields | select_expr
    show_fields | show_expr
    ghost_fields | ghost_expr
    """
    
                 
    if request.vars._grid == 'True':  # GRID
        grid_kwargs = { key: search.get(key) for key in 'left join groupby having'.split() }
        represent4export = { 
            # w2ui_colname(db.auth_user.active): lambda value: represent_boolean(value, html=False)
        } 
        stuff = w2ui_grid_data(
                        search.query,   
                        fields_4columns=fields_4columns, 
                        # fields_4virtual=fields_4virtual, 
                        represent4export=represent4export, 
                        after_select_before_render=after_select_before_render,
                        data_name=data_name, 
                        **grid_kwargs 
                        )
#         response.view = "generic.load"
        return stuff
          
    else:                
        response.view = 'plugin_w2ui_grid/w2ui_grid_deprecated.html'
        
        # some workarounds for grand core stuff
        response.subtitle  = "test  w2ui_grid"
        response.menu = []
        
        context = dict(        
            cid = cid,
            form = search.form,
            w2grid_columns =  [            
                 {'field': w2ui_colname(f), 'caption': f.label, 'size': "100%", 'sortable': isinstance(f, (Field, Expression)), 'resizable': True}
                 # for f in fields_4columns +fields_ghosts  # TODO:  impacts field order -- fields ghosts should go in fields_4cols 
                 for f in fields_4columns # TODO:  impacts field order -- fields ghosts should go in fields_4cols 
             ],
            grid_function = request.function,       # or 'users_grid'     
            data_name = data_name , # request.controller could be default       
            # w2grid_sort = [  {'field': w2ui_colname(db.auth_user.username), 'direction': "asc"} ]
            w2grid_sort = [  {'field': w2ui_colname(db.auth_user.email), 'direction': "asc"} ]
            # ,dbg = response.toolbar()
        )
        return context     
