# -*- coding: utf-8 -*-
from joins_builder import build_joins_chain

"""
for TEST purposes 
"""

"""
Smart joins builder lets you write
    build_joins_chain( ['auth_user', 'auth_membership', 'auth_group', 'auth_permission'] )
instead of
    [
      db.auth_membership.on( db.auth_membership.user_id == db.auth_user.id ),
      db.auth_group.on( db.auth_group.id == db.auth_membership.group_id ),
      db.auth_permission.on( db.auth_permission.group_id == db.auth_group.id ),
    ]
"""

# For test cases I use the default auth DB model, and want to join 4 tables.
# DB model references indicated by arrows:
# auth_user <- auth_membership -> auth_group <- auth_permission

from pprint import pformat


def test_joins_builder(joins):
    fields = ( 
                db.auth_user.id, db.auth_user.first_name, db.auth_user.email, 
                db.auth_membership.id,
                db.auth_group.id, db.auth_group.role,
                db.auth_permission.id, db.auth_permission.name, db.auth_permission.table_name, 
             )
    headers = {str(field):str(field).replace('.', ".\n") for field in fields}
    
    data = SQLFORM.grid(db.auth_user.id > 0, headers=headers, fields=fields,  # can toggle comment \/
    # data = db(db.auth_user.id > 0).select( *fields,                         # can toggle comment ^

            left = build_joins_chain( *joins )              ###  BUILD JOINS !  ###

        )
        
    menu4tests()
    return dict( 
                joins = PRE(pformat(joins)),
                data = data, 
                sql = XML(db._lastsql[0].replace('LEFT JOIN', '<BR/>LEFT JOIN '))   # shows the joins ;)
               )
    

def test1_with_simple_tables_or_fields(): # OK
    """
    it guess'es joining fields from DB model ;)
    table can be either str or DAL table
    """
    return test_joins_builder( 
        joins =  ['auth_user', db.auth_membership, 'auth_group', db.auth_permission.group_id]  
    )


def test2_with_mixed_indication_and_expr(): # test with usual expression -- OK
    return test_joins_builder( 
        joins =  [  'auth_user',                # table name as str
                    db.auth_membership,         # table as DAL
                    db.auth_group.on( db.auth_group.id == db.auth_membership.group_id),  # standart expr
                    db.auth_permission.group_id # Field as DAL
                  ] 
    )


def test3_with_indication_of_fields():  # OK
    """
    if you want to be sure the right fields are used, 
    you can indicate them in tuples 
    """
    return test_joins_builder( 
        joins = [ (None,  db.auth_user.id),             # could be just:   db.auth_user.id
                  (db.auth_membership.user_id, db.auth_membership.group_id),  
                  (db.auth_group.id, db.auth_group.id), 
                  (db.auth_permission.group_id, None)    # could be just:   db.auth_permission.group_id
                ] 
        # joins = ['auth_user', (db.auth_membership, 'user_id', None) ] 
    )


def test4__table_and_fields():  # OK
    """
    you can also tell: table[name], field to previous, field to next 
    """
    return test_joins_builder( 
        joins = [ (db.auth_user, None, 'id'), 
                  (db.auth_membership, 'user_id', 'group_id'),
                  (db.auth_group, 'id', 'id'), 
                  (db.auth_permission, 'group_id', None)
                ] 
        # joins = ['auth_user', (db.auth_membership, 'user_id', None) ] 
    )

def test5_table_alias():  # seems OK     # TODO -- better parse alias'es ;)
    
    # BUG SQLFORM.grid   doesn't give any row if auth_user is aliased
     
    # http://www.web2py.com/books/default/chapter/29/06/the-database-abstraction-layer#Self-Reference-and-aliases
    
     # Aliases -- could be toggled by comments 
    user = db.auth_user.with_alias('user')
    # user = db.auth_user
    membership = db.auth_membership.with_alias('membership')  
    # membership = db.auth_membership
    group = db.auth_group.with_alias('group')
    # group = db.auth_group
    
    fields = ( 
                user.id, user.first_name, user.email, 
                membership.id, # db.auth_membership.with_alias('membership').id,  
                group.id, group.role,
             )
    headers = {str(field):str(field).replace('.', ".\n") for field in fields}
    
    # data = SQLFORM.grid(user.id > 0, headers=headers, fields=fields,  # can toggle
    data = db(user.id > 0).select( *fields,          # can toggle

            left = build_joins_chain( user, membership, group ) 
        )
        
    menu4tests()
    return dict( 
                joins = PRE(pformat([user, membership, group])),
                data = data, 
                sql = XML(db._lastsql[0].replace('LEFT JOIN', '<BR/>LEFT JOIN '))   # shows the joins ;)
               ) 


#-----------
def populate_fake_auth():
    """call it if testing on empty app """
    from gluon.contrib.populate import populate
    # 'auth_user <- auth_membership -> auth_group <- auth_permission
    for table in 'auth_user auth_group auth_permission auth_membership'.split():
        populate(db[table],5)    
        db.commit()




controller_dir = dir()
def menu4tests():
    test_functions = [x for x in controller_dir if x.startswith('test') and x!='test_joins_builder']    
    response.menu = [('TESTS', False, '', 
                        [  
                            (f, f==request.function, URL(f) )
                            for f in test_functions
                        ]
                    )]
    return response.menu



def index():  
    menu4tests()
    return dict(menu=MENU(response.menu))
    
