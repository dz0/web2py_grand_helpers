# -*- coding: utf-8 -*-

# versijų palyginimas https://docs.google.com/document/d/1BEUqSpBhQx781L-HMDxN5A2S2_4GaEmAsFTbS8h8sZU/edit

from plugin_search_form.search_form import SearchField, SearchForm
from plugin_joins_builder.joins_builder import build_joins_chain 
from plugin_w2ui_grid.w2ui_grid import w2ui_grid_data, w2ui_colname, define_w2ui_columns
from plugin_w2ui_grid.w2ui_grid import inject_attrs


from gluon.sqlhtml import represent
from gluon.storage import Storage
from pydal.objects import Field, Expression, Table

from searching import search_form
from lib.w2ui import make_orderby, save_export, serialized_lists
from applications.app.modules.searching import search_form

@auth.requires_permission('list', 'user')
# @auth.requires_signature()
def users():  

    filters = [
        {'label': T('core__all'), 'data': {}},
        {'label': T('user__active'), 'selected': True, 'data': {'active': 'T'}},
        {'label': T('user__inactive'), 'data': {'active': 'F'}}
    ]

    cid = request.function 
    data_name = 'user' # for checking permissions, etc..

    # SEARCH FORM
    
    custom_active = db.auth_user.active
    custom_active.requires=IS_IN_SET([(True, T('core__yes')), (False, T('core__no'))], zero=T('core__all'))
    custom_active.widget=SQLFORM.widgets.options.widget
    
    search = SearchForm( 
        [db.auth_user.username, db.auth_user.type],
        [db.auth_user.email, db.auth_user.data_level],
        [custom_active,   db.branch_staff.branch_id  ],

        hidden={
            'users_autocomplete_username': URL('user', 'autocomplete_username.json'),
            'users_autocomplete_email': URL('user', 'autocomplete_email.json')
        },
        
        table_name='auth_user',
        filters=filters,
        formstyle='divs' if IS_MOBILE else None,          # formstyle='table3cols',
        _class='mobile_solidform' if IS_MOBILE else None,

        form_factory = lambda *a, **kw:  search_form(cid, *a, **kw), # pass cid to form facotry
        
        # w2ui
        _name = '{0}__form'.format(cid),
        _action = 'javascript:w2ui__grid("{0}");'.format(cid)
    )

    # GRID COLUMNS MODEL -- fields_4columns
    
    fields_4columns=[   
                # db.auth_user.id,
                db.auth_user.username,
                db.auth_user.email,
                db.auth_user.active,
                db.auth_user.type, 
                db.auth_user.data_level, 
                    ] 

    def whole_page():
        response.view = 'plugin_w2ui_grid/w2ui_grid.html'
        
        # some workarounds for grand core stuff
        response.subtitle = T('user__list_form')
        response.files.append(URL('static', 'user/js/users.js'))
        response.files.append(URL('static', 'user/js/add_user.js'))
        # response.menu = []
        
        return dict(        
            cid = cid,
            form = search.form,
            w2grid_columns = define_w2ui_columns( fields_4columns ), # will generate defaults:  {'field': w2ui_colname(f), 'caption': f.label, 'size': "100%", 'sortable': isinstance(f, (Field, Expression)), 'resizable': True}
            grid_function = request.function,       # or specify other...     
            data_name = data_name , # request.controller could be default       
            context = request.controller +'_'+ data_name,

            w2grid_sort = [  {'field': w2ui_colname(db.auth_user.email), 'direction': "asc"} ]
        )
     
    # @auth.requires_signature()
    def grid_data():  # uses search.query  and   fields_4columns 
           
        grid_kwargs_from_search = { key: search.get(key) for key in 'left join groupby having'.split() }
        
        return w2ui_grid_data(
                        search.query,   
                        fields_4columns=fields_4columns, 
                        # after_select_before_render=None, 
                        data_name=data_name, 
                        **grid_kwargs_from_search 
                        )
                  
    if request.vars._grid == 'True':  
        return grid_data()
    else:
        return whole_page()




@auth.requires_login()
def autocomplete_username():
    records = db(db.auth_user.username.contains(request.vars.term)).select(
        db.auth_user.username,
        groupby=db.auth_user.username,
        orderby=db.auth_user.username
    )
    result = [r.username for r in records]
    return json(result)


@auth.requires_login()
def autocomplete_email():
    records = db(db.auth_user.email.contains(request.vars.term)).select(
        db.auth_user.email,
        groupby=db.auth_user.email,
        orderby=db.auth_user.email
    )
    result = [r.email for r in records]
    return json(result)


@auth.requires_signature()
def users_grid():
    cid = request.vars.cid
    status = 'success'
    cmd = request.vars.cmd

    if cmd in ('get-records', 'export-records'):
        if not auth.has_permission('list', 'user'):
            return {'status': 'error', 'message': MSG_NO_PERMISSION}

        orderby = None
        limitby = None
        records = []

        queries = [db.auth_user.id > 0]
        if request.vars.username:
            queries.append(db.auth_user.username.contains(request.vars.username))
        if request.vars.email:
            queries.append(db.auth_user.email.contains(request.vars.email))
        if request.vars.active:
            status = {'T': True, 'F': False}
            queries.append(db.auth_user.active == status[request.vars.active])
        if request.vars.data_level:
            queries.append(db.auth_user.data_level == request.vars.data_level)
        if request.vars.type:
            queries.append(db.auth_user.type == request.vars.type)
        if request.vars.branch_id:
            queries.append(db.auth_user.id == db.branch_staff.user_id )  # join
            queries.append(db.branch_staff.branch_id == request.vars.branch_id)  # search
        query = reduce(lambda a, b: (a & b), queries)

        extra = serialized_lists(request.vars)  # sort, search
        if 'sort' in extra:
            orderby = make_orderby(db, extra['sort'], table_name='auth_user')

        if cmd == 'get-records':
            offset = int(request.vars.offset)
            limit = int(request.vars.limit)
            limitby = (offset, offset + limit)

        rows = db(query).select(
            db.auth_user.ALL, TOTAL_ROWS,
            orderby=orderby,
            limitby=limitby
        )

        if cmd == 'export-records':
            db.auth_user.active.represent = lambda value: represent_boolean(value, html=False)

        for r in rows:
            records.append({
                'recid': r.auth_user.id,
                'username': represent(db.auth_user.username, r.auth_user.username, r.auth_user),
                'email': represent(db.auth_user.email, r.auth_user.email, r.auth_user),
                'active': represent(db.auth_user.active, r.auth_user.active, r.auth_user),
                'type': represent(db.auth_user.type, r.auth_user.type, r.auth_user),
                'data_level': represent(db.auth_user.data_level, r.auth_user.data_level, r.auth_user)
            })

        if cmd == 'get-records':
            total = rows[0][TOTAL_ROWS] if rows else 0
            return {'status': status, 'total': total, 'records': records}

        elif cmd == 'export-records':
            labels = {
                'username': db.auth_user.username.label,
                'email': db.auth_user.email.label,
                'active': db.auth_user.active.label,
                'type': db.auth_user.type.label,
                'data_level': db.auth_user.data_level.label
            }
            columns = request.vars.getlist('columns[]')
            data = [[labels[c] for c in columns]]
            for r in records:
                data.append([r[c] for c in columns])

            save_export(cid, data)
            return {'status': status}

    elif cmd == 'delete-records':
        if not auth.has_permission('delete', 'user'):
            return {'status': 'error', 'message': MSG_NO_PERMISSION}

        selected = request.vars.getlist('selected[]')
        try:
            for s in selected:
                del db.auth_user[s]
        except:
            db.rollback()
            return {'status': 'error', 'message': MSG_ACTION_FAILED}
        return {'status': status}

    return {'status': status}



