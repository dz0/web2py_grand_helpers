# -*- coding: utf-8 -*-

################## some auxilary functions 


# def get_databases(request):
    # dbs = {}
    # for (key, value) in globals().items():
        # cond = False
        # try:
            # cond = isinstance(value, GQLDB)
        # except:
            # cond = isinstance(value, SQLDB)
        # if cond:
            # dbs[key] = value
    # return dbs

# databases = get_databases(None)

#### 

# db = get_databases(request)[0]

# from plugin_AnySQLFORM import AnySQLFORM, FormField
from plugin_joins_builder import build_joins_chain

def index():
    # auth_user <- auth_membership -> auth_group 
    chain_tables = [ db.auth_user, db.auth_membership, db.auth_group ]
    
    # joins_chain_tablenames =  db.tables
    tablenames =  [t._tablename for t in chain_tables]
    
    # for tablename in db.tables:
        # for field in db[tablename]:
    def field_selector():
        def fieldnames( fields ):
            return [f.name for f in fields]
            
        return SQLFORM.factory(
                
                 *[ Field( tablename, requires=IS_IN_SET( fieldnames( db[tablename]) , multiple=True ) )    
                        for tablename in  tablenames
                   ],
                 _class="selector"
                
                )
                        
    columns_selector = field_selector()
    columns_selector.process(keepvalues=True)
    def reconstruct_fields(vars):
        return [ db[t][f] for t, fns in  vars.items()  for  f in fns]
    
    search_fields_selector = field_selector()

    # may apply http://www.jqueryscript.net/form/jQuery-Plugin-To-Convert-Select-Options-To-Checkboxes-multicheck.html
    return dict(
                columns_selector=columns_selector,
                columns_selector_vars=columns_selector.vars,
                
                grid = SQLFORM.grid(
                    db.auth_user,
                    fields = reconstruct_fields(columns_selector.vars),
                    left = build_joins_chain( *chain_tables ), 
                    user_signature=False,
                    )
            )
