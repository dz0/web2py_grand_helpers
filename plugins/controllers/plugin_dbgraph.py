# -*- coding: utf-8 -*-

###################### copy/paste from appadmin.py ###############

# ##########################################################
# ## make sure administrator is on localhost
# ###########################################################

import os
import socket
import datetime
import copy
import gluon.contenttype
import gluon.fileutils

try:
    import pygraphviz as pgv
except ImportError:
    pgv = None

is_gae = request.env.web2py_runtime_gae or False

# ## critical --- make a copy of the environment

global_env = copy.copy(globals())
global_env['datetime'] = datetime

http_host = request.env.http_host.split(':')[0]
remote_addr = request.env.remote_addr
try:
    hosts = (http_host, socket.gethostname(),
             socket.gethostbyname(http_host),
             '::1', '127.0.0.1', '::ffff:127.0.0.1')
except:
    hosts = (http_host, )

if request.is_https:
    session.secure()
elif (remote_addr not in hosts) and (remote_addr != "127.0.0.1"):
    # and (request.function != 'manage'):
    raise HTTP(200, T('appadmin is disabled because insecure channel'))

################## some auxilary functions 

def get_databases(request):
    dbs = {}
    for (key, value) in global_env.items():
        cond = False
        try:
            cond = isinstance(value, GQLDB)
        except:
            cond = isinstance(value, SQLDB)
        if cond:
            dbs[key] = value
    return dbs

databases = get_databases(None)

#############  DIFF's start here ####################

def index(): redirect( URL( 'graph_model' ))

def table_template(table):
    from gluon.html import TR, TD, TABLE, TAG

    def FONT(*args, **kwargs):
        return TAG.font(*args, **kwargs)

    def types(field):
        f_type = field.type
        if not isinstance(f_type,str):
            return ' '
        elif f_type == 'string':
            return field.length
        elif f_type == 'id':
            return B('pk')
        elif f_type.startswith('reference') or \
                f_type.startswith('list:reference'):
            return B('fk')
        else:
            return ' '

    def type_repr(field):
        result = field.type
        if 'reference' in field.type:
            result = field.type.replace('reference ', '--> ')
            if field.name.endswith('_id')  and field.name[:-3]==result[len('--> '):] :
                result = '--> '
        return result
        
    # This is horribe HTML but the only one graphiz understands
    rows = []
    cellpadding = 4
    color = "#000000"
    bgcolor = "#FFFFFF"
    face = "Helvetica"
    face_bold = "Helvetica Bold"
    border = 0

    rows.append(TR(TD(FONT(table, _face=face_bold, _color='blue'),  #  _size="20" doesn't work..
                           _colspan=3, _cellpadding=cellpadding,
                           _align="center", _bgcolor=bgcolor)))
    for row in db[table]:
        if is_interesting_field(  row.name +" "+ row.type +" "+ str(types(row)) ):
            rows.append(TR(TD(FONT(row.name, _color=color, _face=face_bold),
                                  _align="left", _cellpadding=cellpadding,
                                  _border=border),
                           TD(FONT(type_repr(row), _color=color, _face=face),
                                   _align="left", _cellpadding=cellpadding,
                                   _border=border),
                           TD(FONT(types(row), _color=color, _face=face),
                                   _align="center", _cellpadding=cellpadding,
                                   _border=border)))
    return "< %s >" % TABLE(*rows, **dict(_bgcolor=bgcolor, _border=1,
                                          _cellborder=0, _cellspacing=0)
                             ).xml()


def is_interesting_field( fieldname ):
    # defaults could be  'pk fk'
    show_fields=(request.get_vars.show_fields or '').replace('%20', ' ').split()
    if not show_fields: return True
    for word in show_fields: 
        if word in fieldname:  
            return True    # include
        if word.startswith('-') and word[1:] in fieldname:
            return False   # or exclude    

def is_important_force_exceptions_first(tablename): # DEPRECATED
    # in views/appadmin.html forward vars=request.vars:  =IMG(_src=URL('appadmin', 'bg_graph_model', vars=request.vars)
    # graph_model?table_filters=...&field_filters=...&show_fields=fk%20pk
    table_filters=(request.vars.table_filters or 'invoice sales_order -blind -good -batch -discount -shipment').replace('%20', ' ').split()
    
    field_filters=(request.vars.field_filters or 'user').replace('%20', ' ').split()
    # if request.vars.filters:
        # filters=request.vars.filters.split()

    # match by table name
    excluding_filters = [word[1:] for word in table_filters if word.startswith('-')]
    for word in excluding_filters: 
        if word in tablename: 
            return False  # force exclude first
            
    for word in table_filters: 
        if word in tablename:  
            return True    # include

    # match by field name
    for field in db[tablename]:
        for word in field_filters: # or one of it's fields' names
            if word in field.name:
                return True

def is_shown(tablename):
    if session.graph_shown_tables:
        return tablename in session.graph_shown_tables
        
def matches_filter(tablename):
    """
    Takes arguments and returns True on first match, 
    minus sign ("-") means, we don't want this match...
    """
    # in views/appadmin.html forward vars=request.vars:  =IMG(_src=URL('appadmin', 'bg_graph_model', vars=request.vars)
    # graph_model?table_filters=...&field_filters=...
    table_filters=(request.get_vars.table_filters or "").replace('%20', ' ').split()
    
    field_filters=(request.get_vars.field_filters or request.get_vars.table_filters or "").replace('%20', ' ').split()
    
    if table_filters==[] and field_filters==[]: # if no filters set -- show everything
        return True 
        
    # if request.vars.filters:
        # filters=request.vars.filters.split()

    for word in table_filters: 
        if word in tablename:  
            return True    # include
        if word.startswith('-') and word[1:] in tablename:
            return False   # or exclude

    # match by field name
    for field in db[tablename]:
        for word in field_filters: # or one of it's fields' names
            if word in field.name:
                return True    # include
            if word.startswith('-') and word[1:] in field.name:
                return False   # or exclude

def bg_graph_model():

    if request.vars.action == 'match':
        session.graph_shown_tables = [table for table in db.tables if matches_filter(table)]

    if request.vars.action == 'findpath':
        if (session.findpath['start'] == request.vars.start
        and session.findpath['finish'] == request.vars.finish):
            pass # no need to re-generate stuff
        else:
            session.graph_shown_tables = findpath_between_tables()
        
    if request.vars.action == 'list':
        session.graph_shown_tables = request.vars.tables.replace("%20", " ").replace("->", "").replace("<-", "").split()

    graph = pgv.AGraph(layout='dot',  directed=True,  strict=False,  rankdir='LR')

    subgraphs = dict()
    for tablename in db.tables:
        if hasattr(db[tablename],'_meta_graphmodel'):
            meta_graphmodel = db[tablename]._meta_graphmodel
        else:
            meta_graphmodel = dict(group=request.application, color='#ECECEC')

        group = meta_graphmodel['group'].replace(' ', '')
        if not subgraphs.has_key(group):
            subgraphs[group] = dict(meta=meta_graphmodel, tables=[])
            subgraphs[group]['tables'].append(tablename)
        else:
            subgraphs[group]['tables'].append(tablename)

        graph.add_node(tablename, name=tablename, shape='plaintext',
                       label=table_template(tablename) 
                             if is_shown(tablename) else tablename
                        )
        


    for n, key in enumerate(subgraphs.iterkeys()):
        graph.subgraph(nbunch=subgraphs[key]['tables'],
                    name='cluster%d' % n,
                    style='filled',
                    color=subgraphs[key]['meta']['color'],
                    label=subgraphs[key]['meta']['group'])

    shown_tables = set([])
    for tablename in db.tables:
        for field in db[tablename]:
            f_type = field.type
            if isinstance(f_type,str) and (
                f_type.startswith('reference') or
                f_type.startswith('list:reference')):
                referenced_table = f_type.split()[1].split('.')[0]
                n1 = graph.get_node(tablename)
                n2 = graph.get_node(referenced_table)
                
                if request.vars.neighbours=='0': # show only filtered, &neighbours=0
                    if is_shown(tablename)        :  shown_tables.add( tablename )
                    if is_shown(referenced_table) :  shown_tables.add( referenced_table )
                    if is_shown(tablename) and is_shown(referenced_table):
                        graph.add_edge(n1, n2, color="#4C4C4C", label='')
                else: # default: show neighbours
                    if is_shown(tablename) or is_shown(referenced_table):
                        shown_tables.add( tablename )
                        shown_tables.add( referenced_table )
                        graph.add_edge(n1, n2, color="#4C4C4C", label='')
                    

    # import rpdb2; rpdb2.start_embedded_debugger("a")
    # from gluon.debug import dbg;  dbg.set_trace() # stop here
    for tablename in db.tables:
        if not tablename in shown_tables:
            graph.delete_node( tablename )

    graph.layout()
    if not request.args:
        # response.headers['Content-Type'] = 'image/png'
        # return graph.draw(format='png', prog='dot')
        response.headers['Content-Type'] = 'image/svg+xml'
        return graph.draw(format='svg', prog='dot')
    else:
        response.headers['Content-Disposition']='attachment;filename=graph.%s'%request.args(0)
        if request.args(0) == 'dot':
            return graph.string()
        else:
            return graph.draw(format=request.args(0), prog='dot')

def build_graph():
    dgraph = { tablename: {'to':[], 'fk':{}, 'from':[]} for tablename in db.tables }
     
    for tablename in db.tables:
        for field in db[tablename]:
            f_type = field.type
            if isinstance(f_type,str) and (
                f_type.startswith('reference') or
                f_type.startswith('list:reference')):
                referenced_table = f_type.split()[1].split('.')[0]
                dgraph[tablename]['to'].append( referenced_table )
                dgraph[tablename]['fk'][ referenced_table ] = field.name
                dgraph[referenced_table]['from'].append( tablename )
                # n1 = graph.get_node(tablename)
                # n2 = graph.get_node(referenced_table)
                # graph.add_edge(n1, n2, color="#4C4C4C", label='')
    return dgraph    
   
# http://localhost:8000/app/appadmin/smart_join
# BUG? 
# http://localhost:8000/app/appadmin/graph_model?start=sales_order&finish=address_country&max_joins=6&show_fields=-&neighbours=0&action=findpath#
# neranda visų kelių (tame tarpe teisingo - subject_address)
# http://localhost:8000/app/appadmin/graph_model?tables=sales_order+-%3E+branch+%3C-+subject_settings+-%3E+address_country++subject_address+sales_order+-%3E+branch+%3C-+subject_settings+-%3E+address_country++address_address&show_fields=-&neighbours=0&action=list#
def findpath_between_tables(start=request.vars.start, finish=request.vars.finish, max_joins=request.vars.max_joins):
    dgraph = build_graph()
    
    # use breadth-first search
    queue = [start]
    paths = [ [start] ] # list of names
    # paths_refs = [ [""] ]
    # result = []
    result_paths = []
    # visited = [ start ]

    
    while queue:
        table = queue.pop(0)
        path = paths.pop(0)
        # refs = paths_refs.pop(0)
        
        
        if table == finish:  # reach finish
            result_paths.append( path )
            continue

        if len(path) > int(max_joins):  # if path would become too long
            continue
        
        # from gluon.debug import dbg
        # dbg.set_trace() # stop here!            
        
        node = dgraph[ table ]
        for x in node['to']+node['from']: # look for join in any direction
            if (not x in path) or x==finish: # exception for x==finish will let find many paths (probably not needed after deprecating visited)
                queue.append( x )
                paths.append( path+[x] )
                # visited.append( x )
           
    # joins from path (using foreign key names)
    
    def repr_joins( path, style='w2p' or 'str' ):
        
        def add_join(A, B):
            def make_join_w2p(added_table, pk_table, fk_table, fk): 
                # return db[added_table].on( db[pk_table].id == db[fk_table][fk] )
                join_txt = "db[added_table].on( db[pk_table].id == db[fk_table][fk] )"
                # hack to generate nicer txt
                for var in "added_table, pk_table, fk_table, fk".split(", "):
                    join_txt = join_txt.replace("[%s]"%var, "."+locals()[var])
                return  join_txt
                
            if style=='w2p':
                if B in dgraph[A]['to']:
                    joins.append( make_join_w2p(B, B, A, dgraph[A]['fk'][B]) )
                else:
                    joins.append( make_join_w2p(B, A, B, dgraph[B]['fk'][A]) )

            if style=='str':
                if B in dgraph[A]['to']:
                    joins.append( " -> "+B )
                else:
                    joins.append( " <- "+B )

        joins = []    
        for i in range(len(path)-1):
            A = path[i]
            B = path[i+1]
            add_join( A, B )
            
        return joins
    from pprint import pformat
    session.findpath = {'start':start, 'finish':finish}
    # session.findpath['joins'] = {str(path): generate_join_chain(path) for path in result_paths}
    def path_hash(path): return path[0]+''.join(repr_joins(path, 'str'))  if path else None
    # constructs sth like: invoice -> auth_user <- sales_settings -> good_category
    session.findpath['joins'] = { path_hash(path) : [PRE(', \n'.join(repr_joins(path, 'w2p'))), 
                                                     A('(url)',  _href= URL(vars={'action':'list', 'tables':path_hash(path) }))
                                                    ] 
                                    for path in result_paths}
    
    
    # return list(set(sum(result_paths)))  # return set of names from paths
    
    return list(set(reduce(lambda a, b: a+b, result_paths))) if result_paths else []  # return set of names from paths

def smart_join(): 
    return dict(path=findpath_between_tables()) #alias

# src= https://gist.github.com/dz0/ef4bea4e6f4aaf21f084c06190efecf6
def graph_model():

    session.graph_model_vars = session.graph_model_vars or {}  #init stuff for request vars
    if request.vars:   session.graph_model_vars.update( request.post_vars or request.get_vars) ;
    previous = session.graph_model_vars  # prepare to fill form fields with earlier info
    

    match_form = SQLFORM.factory(
        Field('table_filters',  default=previous.get('table_filters', '') ), 
        Field('field_filters',  default=previous.get('fields_filters', '') ), 
        Field('show_fields',  default=previous.get('show_fields', 'fk') ), 
        Field('neighbours', "integer", default=previous.get('neighbours', 0 )), 
        Field('action',  default="match"), 
        _method="GET",
    )
    
    findpath_form = SQLFORM.factory(
        Field('start',  default=previous.get('start', ''), requires=IS_IN_SET( list(db.tables))     ), 
        # Field('bla', default=127, widget = SQLFORM.widgets.autocomplete(request, db.good.sku, id_field=db.good.id) )
        Field('finish',  default=previous.get('finish', '') , requires=IS_IN_SET( list(db.tables))), 
        Field('max_joins', "integer", default=previous.get('max_joins', 3) ), 
        Field('show_fields',  default=previous.get('show_fields', 'fk') ), 
        Field('neighbours', "integer", default=previous.get('neighbours', 0 )), 
        Field('action', default="findpath"), 
        _method="GET",
    )
    
    if request.vars.action == 'findpath':
        try:
            session.graph_shown_tables = findpath_between_tables()
        except Exception as e:
            response.flash += "Error: Can't 'findpath_between_tables':\n"+str(e)

    
    list_form = SQLFORM.factory(
        Field('tables',  default=previous.get('tables', ''),   ), 
        Field('show_fields',  default=previous.get('show_fields', 'fk') ), 
        Field('neighbours', "integer", default=previous.get('neighbours', 0 )), 
        Field('action', default="list"), 
        _method="GET",
    )
      
    # highlight currently submitted  form
    for form in [match_form, findpath_form, list_form]:
        form_action = form.element('input',_name='action')['_value']
        if form_action :
            if form_action == request.vars.action:
                # response.flash+= form_action+ " \n "
                form['_style']="margin: 5px; padding:5px; border:1px gray solid;"
                # form.element('table')['_style']="background-color: #E5E5E5"
            
    return dict(databases=databases, pgv=pgv, forms=[findpath_form, list_form, match_form])
