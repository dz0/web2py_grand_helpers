# -*- coding: utf-8 -*-


###### models ##########
# a way to make views/registers persistant 

db.define_table('plugin_dbmanager_view',
    Field('article', 'string'),
    Field('main', 'boolean'),
    Field('order_no', 'integer', label='#'
            # , 
            # default=lambda: create_order_no(db, 'language'),
            # represent=lambda value: represent_order_no(db, 'language', value, 'sticker_languages')
          ),
    format='%(article)s'
)

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
                
                   Field( 'exprs', label="SQL Expressions (one per line)", widget=SQLFORM.widgets.text.widget), 
                 *[ Field( tablename, requires=IS_IN_SET( fieldnames( db[tablename]) , multiple=True ) )    
                        for tablename in  tablenames
                   ],
                 _class="selector" 
                 # ,_method="GET"  # affects   process().vars
                
                )
                        
    columns_selector = field_selector()
    columns_selector.process(keepvalues=True)
    
    def get_sql_expressions(vars):
        """
        if one enters:
        auth_user.id*2
        count(auth_group.id)
        
        and selects auth_user.first_name
        and groupby=db.auth_user.id

        SELECT auth_user.first_name, auth_user.id*2 , count(auth_group.id) FROM auth_user LEFT JOIN auth_membership ON (auth_membership.user_id = auth_user.id) LEFT JOIN auth_group ON (auth_group.id = auth_membership.group_id) WHERE (auth_user.id IS NOT NULL) GROUP BY auth_user.id;
        """
        if 'exprs' in vars:
            return [s for s in vars['exprs'].split('\n') if s.strip() ]
        return []
    
    
    def construct_fields(vars):
        return [ db[t][f] for t, fns in  vars.items()  for  f in fns if t in db]
    
    search_fields_selector = field_selector()

    def grid(sql=False):

            # grid = SQLFORM.grid(
                # chain_tables[0],
                # #field_id = chain_tables[0]._id,
                # fields = construct_fields(columns_selector.vars) + get_sql_expressions(columns_selector.vars),
                # left = build_joins_chain( *chain_tables ), 
                # user_signature=False,
            # ),
            
            # ordinary select
            dbset = db(chain_tables[0]) 
            adaptive_select = dbset._select if sql else dbset.select # choose if we return sql or data
            
            grid = adaptive_select(
                *(construct_fields(columns_selector.vars) + get_sql_expressions(columns_selector.vars)),
                left = build_joins_chain( *chain_tables ) 
                ,groupby=db.auth_user.id  # hardcoded for testing
            )
            
            return grid


    # may apply http://www.jqueryscript.net/form/jQuery-Plugin-To-Convert-Select-Options-To-Checkboxes-multicheck.html
    return dict(
                columns_selector=columns_selector,
                columns_selector_vars=columns_selector.vars,
                custom_sql_expressions=get_sql_expressions(columns_selector.vars),
                grid=grid(),
                SQL=grid(sql=True)
            )
