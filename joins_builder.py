# -*- coding: utf-8 -*-


"""
Smart joins builder lets you write
    build_joins( ['auth_user', 'auth_membership', 'auth_group', 'auth_permission'] )
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


def test_joins_builder(joins):
    fields = ( 
                db.auth_user.id, db.auth_user.first_name, db.auth_user.email, 
                db.auth_membership.id,
                db.auth_group.id, db.auth_group.role,
                db.auth_permission.id, db.auth_permission.name, db.auth_permission.table_name, 
             )
    headers = {str(field):str(field).replace('.', ".\n") for field in fields}
    
    data = SQLFORM.grid(db.auth_user.id > 0, headers=headers, fields=fields,  # can toggle
    # data = db(db.auth_user.id > 0).select( *fields,          # can toggle

            left = build_joins( joins )              ###  BUILD JOINS !  ###

        )
    
    return dict( data = data, 
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

def test5_alias():  # seems OK     # TODO -- better parse alias'es ;)
    
    # http://www.web2py.com/books/default/chapter/29/06/the-database-abstraction-layer#Self-Reference-and-aliases
    
    # user = db.auth_user.with_alias('user')
    user = db.auth_user
    membership = db.auth_membership.with_alias('membership')  # create alias
    # membership = db.auth_membership
    group = db.auth_group.with_alias('group')
    # group = db.auth_group
    
    fields = ( 
                user.id, user.first_name, user.email, 
                membership.id, # db.auth_membership.with_alias('membership').id,  # Alias
                 
                group.id, group.role,
             )
    headers = {str(field):str(field).replace('.', ".\n") for field in fields}
    
    data = SQLFORM.grid(user.id > 0, headers=headers, fields=fields,  # can toggle
    # data = db(True).select( *fields,          # can toggle

            left = build_joins([ 
                                 user, # aliased_user
                                 (membership, 'user_id', 'group_id'), # db.auth_membership.with_alias('membership') ,   # ! With Alias
                                 # membership, # db.auth_membership.with_alias('membership') ,   # ! With Alias
                                 group
                               ])              ###  BUILD JOINS !  ###

        )
    
    return dict( data = data, 
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




# For alpha stage 
# I keep helper logic in controller for easier testing // moving around projects.. 

if "SMART JOINS BUILDER":  # fold it :)
    # from __future__ import print_function  # needed in my env for  web2py shell    
    def myprint(*args):
        # print( *args )
        print args 


    from collections import defaultdict
    from gluon.storage import Storage

    def find_references_and_fkeys( table ):
        """
        returns set/dict of referenced tables (associated with grouped foreign keys)
        
        ps.: in most cases  there is only one FK for referenced table, but sometimes you kave several
        """
        result = defaultdict(list) 
        for field in db[table]:
            f_type = field.type
            if isinstance(f_type,str) and (
                f_type.startswith('reference') or
                f_type.startswith('list:reference')):
                referenced_table = f_type.split()[1].split('.')[0]
                result[ referenced_table ].append( field.name )            
        return result

    
    def update_reference_map_with_table(tablename, force_update=False):
        table = db[tablename]
        
        refs = db_reference_map()
        
        if not tablename in refs or force_update:
            db.tablenames.append( tablename )  # would be enough -- as rest could be done by renenerating db_reference_map()
            
            refs[tablename]=find_references_and_fkeys(tablename)
            
            for from_ in table._referenced_by:  # list of fields that reference the table
                #         from            to            by FK
                refs[from_._tablename][tablename].append( from_.name )
                
        return refs        # save in singleton or so
        
    def db_reference_map():
        # should be used as singleton -- might cache or store in session?
        # but invalidated or updated on table alias!  (after db.tablenames.append( tablename ))
        
        return{ x: find_references_and_fkeys(x) for x in db.tables }
         
     
        
    ## smart join smart_join feature prototype

    def find_or_check_connection( A, B, A_field=None, B_field=None ):
        """
        A and B are Storages
        Returns pair/tuple:   A_field,  B_field
        One of fields can be given, then we look for the missing one.
        """
        
        def make_sure__single_ref( refs ):
            ref_count = ( len(refs) )
            if ref_count != 1:
                msgAmount = "None" if ref_count==0 else ("Too many (%s) possible"%ref_count)
                # raise TypeError(msgAmount+" references between tables: %s  -- %s " % (A+A_field , B)  )
                raise TypeError(msgAmount+" references between tables: %s.%s  -- %s.%s " % (A, A_field, B, B_field)  )
        
        refs = db_reference_map()
        
        # if we already have full info  -- both fields
        if A_field and B_field: 
            # doublecheck if they are OK
            if B_field  == db[B]._id.name and not A_field in refs[A][B] \
            or A_field == db[A]._id.name and not B_field in refs[B][A] \
            or A_field == db[A]._id.name and B_field == db[B]._id.name \
            or A_field != db[A]._id.name and B_field != db[B]._id.name :
                raise ValueError("Wrong given join fields: %s.%s  -- %s.%s" % (  A,  A_field, B, B_field ) )            
            return A_field, B_field   
            
        # if we have partial info -- one of fields
        
        # if it is FK
        if A_field:
            if A_field in refs[A][B]:
                return A_field, db[B]._id.name
            else:  
                raise ValueError("Wrong given join field:  %s.%s -- %s" % (  A, A_field, B) )  
        
        if B_field:
            if B_field in refs[B][A]:
                return db[A]._id.name, B_field
            else:
                raise ValueError("Wrong given join field:  %s -- %s.%s" % (  A, B, B_field  ) )  

        # if it is PK 
        # in rare cases, it makes sense, for example: 
        # if  A has reference to B   and   B has reference to A   at the same time. (then giving 'id' narrows possibilities)      
        
        if A_field == db[A]._id.name: 
            make_sure__single_ref( refs[B][A] ) # look for FK in B  
            return db[A]._id.name, refs[B][A][0]
            
        if B_field == db[B]._id.name:   # look for FK in A 
            make_sure__single_ref( refs[A][B] )
            return refs[A][B][0], db[B]._id.name

        # find out both fields
        if not A_field and not B_field:
            make_sure__single_ref( refs[A][B] + refs[B][A] )

            # if there is exactly one reference -- use it
            if refs[A][B]:  #  foreign key in A  
                return  refs[A][B][0], db[B]._id.name
                
            if refs[B][A]:   #  fk in B  
                return  db[A]._id.name, refs[B][A][0]   
                
                
    from gluon.dal import Expression, Table, Query
    # from gluon.packages.dal.pydal._globals import  DEFAULT
    # from gluon.dal import Expression

    def build_joins( path ):
        """
        first item in path is supposed to come from initial query/select
        Path can contain table|field names (see subfunction parse(..))
        
        We can also use Expression db.table.on(...) in Path -- 
        Hopefully this will alow aliases (not tested)
        
        Examples:
        No fields 
        >>> ( build_joins( ['auth_user', 'auth_membership', 'auth_group']) )
        
        >>> ( build_joins( [db.auth_user, db.auth_membership, 'auth_group']) )
        
        >>> ( build_joins( [db.auth_user, db.auth_membership, db.auth_group, db.auth_permission]) )
        
        Many to many with both fields
        >>> ( build_joins( ['auth_user', ('auth_membership', 'user_id', 'group_id'), 'auth_group']) )
        
        >>> ( build_joins( ['auth_user', (db.auth_membership, 'user_id', 'group_id'), 'auth_group']) )
        
        Many to many with one field
        >>> ( build_joins( ['auth_user', ('auth_membership', None, 'group_id'), 'auth_group']) )
        
        >>> ( build_joins( ['auth_user', (db.auth_membership, 'user_id', None), 'auth_group']) )
        
        First with just right field
        >>> ( build_joins( [ (db.auth_membership, None, 'group_id'), 'auth_group']) )
        
        Last with just left field
        >>> ( build_joins( ['auth_user', (db.auth_membership, 'user_id', None)]) )
        
        
        
        ### more experimental 
        >>> ( build_joins( [db.auth_user, db.auth_membership.user_id ]) )
        
        >>> ( build_joins( [db.auth_user, db.auth_membership.user_id, db.auth_group ]) )
        
        >>> ( build_joins( ['auth_user', db.auth_membership.group_id, 'auth_group']) )

        #auth_user <- auth_membership -> auth_group <- auth_permission
        
        >>> ( build_joins( ['auth_user', db.auth_membership.group_id, 'auth_group', db.auth_permission.group_id]) )
        
        # Expresion
        >>> ( build_joins( ['auth_user', db.auth_membership, db.auth_group.on(db.auth_group.id == db.auth_membership.group_id), db.auth_permission.group_id]) )
        
        """
        
        if len(path) < 2: raise ValueError("There should be at least 2 tables mentioned in %s" % path )
        
        prev = None
        nr = 0
        def parse( item ):
            
            myprint ("\nDGB:", item)
            """
            item can be either of 1, 2, or 3 parts (tuple or just table/field/string (but it must include tablename at least))
            There will be lots of inference based on situation
            
            One:
            >>> parse( 'tablename' ) 
            >>> parse( db.table ) 
            >>> parse( db.table.field ) 
            
            Two parts:
            >>> parse( (db.table.field1,  'field2' ) )
            >>> parse( (db.table.field1,  db.table.field2 ) )
            
            Three parts:
            >>> parse( (db.table, 'field1',  'field2' ) )
            
            With Alias:
            >>> parse( (db.table.with_alias('bla'), 'field1',  'field2' ) )
            
            
            Finds out the connection between previous and current table
            returns current table  and  possibly modifies prev.left_field
            if needed, finds out connection: prev right_field  to current  left_field 
            
            """
            left_field = right_field  = undecided_field = None
            alias = None
            if isinstance(item, tuple) and len(item)==3:  # 3 --  table, field2prev, field2next
                table, left_field, right_field = item
                table = str(table)
                
            if isinstance(item, tuple) and len(item)==2: # 2 --  field2prev, field2next  
                left_field, right_field = item
                count = sum([1  for field in left_field, right_field   if  type(field) is Field] )
                if count == 0:
                    raise TypeError( "None of %s is <Field ...> type -- can't find out table" % (item,) )
                for field in left_field, right_field :
                    if  type(field) is Field:
                        table = field._tablename

            # make sure values of fields are strings 
            def just_field_name(field):
                """
                expects dal.Field or string (or None)
                """
                if type(field) is Field:
                    return field.name 
                else:
                    return field
               
            
            left_field  = just_field_name(left_field)   
            right_field = just_field_name(right_field)
                
            if isinstance(item, tuple) and len(item)==1: # just in case
                item = item[0]
            # if type(item) in [Field, Table, str]:   #  might happen MockTable or WeirdField
            if isinstance(item,  (Field, Table, str) ):   # 1
                table = str(item)
                
                left_field = right_field = None
                myprint( "DBG", table)
                
                if '.' in table: 
                    
                    table, undecided_field = table.split('.')   # will try  find_or_check_connection with  undecided as left and as right
                    if prev is None:
                        right_field = undecided_field
                        undecided_field = None
                    # TypeError("Should be just table (without field), found %s" % item) # TODO could take field and later on decide if it joins to Left or Right
                
            # current = Storage( table=table, left_field=left_field, right_field=right_field, undecided_field=undecided_field )   

            # Check if table is ALIASed
            if " AS " in table and len( table.split(" AS ")) == 2 :  # TODO -- now depends on adapter.. should check earlier and get alias from DAL attrs
                table, alias = table.split(" AS ")
                table = table.strip('"').strip("'")
                myprint( "DBG: aliased", alias, "from", table )
                # db.tables.append( alias  )
                # update_reference_map_with_table( alias )                
                
            if db[table]._ot and db[table]._ot.strip('"') != db[table]._tablename:  # ALIAS  if adapter doesn't say "AS"
                alias, table = table, db[table]._ot.strip('"')
                # db.tables.append( table  )
                # update_reference_map_with_table( table )


            if prev: # if previous exist - if not the starting table in chain
                if undecided_field:
                    try:  # try undecided as left (B_field parameter)
                        prev.right_field, left_field = find_or_check_connection( A=prev.table, B=table, B_field=undecided_field) #
                        # if everything OK
                        undecided_field = None
                    except ValueError as e:
                        myprint ('"Undecided" field not OK for left: "', e)
                        left_field  = None
                        
                        # if item == path[-1]:  # was error with ['auth_user', db.auth_membership.group_id, 'auth_group', db.auth_permission.group_id]  as group_id`s from different tables seemd to think they are equal
                        if nr == len(path)-1: 
                            raise ValueError('"Undecided"/unmatched field in last table %s'% item)      
                                           
                        right_field  = undecided_field  # try undecided as right: will trigger in next call

                        
                try:
                    # myprint( "\nfind_or_check_connection ", ( prev.table, table, prev.right_field, left_field ) )
                    prev.right_field, left_field = find_or_check_connection( prev.table, table, prev.right_field, left_field ) # if we don't know left or any fields
                except ValueError as e:
                    if prev.undecided_field: # check if error was probably because of undecided field
                        prev.right_field = None
                        raise ValueError ('"Undecided" field not matched for %s.%s'%(prev.table, prev.undecided_field)+"\n"+str(e) )
                    else: 
                        raise e 
                    
            current = Storage( alias=alias, table=table, left_field=left_field, right_field=right_field, undecided_field=undecided_field )   
            return current 
            
        # from gluon.debug import dbg
        # dbg.set_trace()
        prev = parse( path[0] ) # previous table # TODO - what if it is Expression with alias?
        
        myprint ("\n",map(str, path), "\n") # DBG
        joins = []
        for item  in path[1:]:
            nr += 1
            # if isinstance(item, Expression):  would be right for field as well ? :/
            if type(item) is Expression: # but not Field...  # we can include prepaired ON's # TODO: maybe check if Expression really is "ON" ?  
                joins.append( item )
                current = Storage( table = str(item.first) )  # hopefully would work with alias?
            else:
                current = parse( item ) # current table
                # myprint "\n", current, "\n", prev # DBG
                c = current.alias or current.table
                p = prev.alias or prev.table
                myprint( "db.%s.on( %s == %s ) " % ( c, db[c][current.left_field], db[p][prev.right_field] )  )
                joins.append( db[c].on( db[c][current.left_field] == db[p][prev.right_field] ) )
            # myprint current
            prev = current
        # dbg.stop_trace()
        return joins


    def  fields(table, field_names):
        return [ str(db[table][fname]) for fname in field_names]
    
