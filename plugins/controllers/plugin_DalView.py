from plugin_DalView import *
from pydal.objects import Field #, Row, Expression
from plugin_grand_helpers import save_DAL_log, test_fields

from  plugin_joins_builder import build_joins_chain






def test_70_group_by_val():
    rows = db().select(db.auth_user.first_name, db.auth_group.ALL,
                left=build_joins_chain( db.auth_user, db.auth_membership, db.auth_group )
                )
    key_field = db.auth_user.first_name
    rows_grouped = rows.group_by_value( key_field )
    # names = rows.column( key_field )

    for name in rows_grouped:
        rows_grouped[name] = [row['auth_group']['role'] for row in  rows_grouped[name] ]
        # rows_grouped[name] = BEAUTIFY(  rows_grouped[name]  )
    return CAT( SQLTABLE(rows), TABLE(map( TR, rows_grouped.items()) ), _border=2  )

def test_70_common_filters():
    db.auth_user._common_filter = lambda q: db.auth_user.id > 100
    query= db.auth_user.email.contains('com')
    return dict( query = db(query).query,
                 sql = db(query)._select(db.auth_user.id, db.auth_user.email )
                 )


def test_80_postgre_distinct():
    # sel = DalView(db.auth_user.first_name, distinct=True, translator=gt) # orderby=db.auth_user.first_name
    sel = DalView(db.auth_user.first_name, distinct=True, translator=None) # orderby=db.auth_user.first_name
    print sel.get_sql()
    return CAT(sel.execute(), PRE(sel.get_sql()) )

def test_00_dev_auth_has_permission():
    return dict(a=auth.has_permission('add', 'auth_user'))

def test_01_virtual_field():
    db.define_table('demo',
                    Field('name')
                    , Field.Virtual('virtual', f=lambda r: 'v...'+r.demo.name, table_name='demo')
                    , Field('bla', default="bla")
                    )
    db.demo.truncate()
    for x in "ABCD":  db.demo.insert( name=x ) # populate

    # return db.demo.fields
    return SQLFORM.grid(db.demo, fields=[db.demo.virtual, db.demo.name])
    # return SQLFORM.grid(db.demo, fields=[db.demo[f] for f in db.demo.fields] +[db.demo.virtual] )
    return db().select(db.demo.ALL)

def init_tables_AB():
    db.define_table('A',  Field('f1'),  Field('f2'),  Field.Virtual('vf3_agg', f=lambda r:None, table_name='A')  )
    db.define_table('B',
                    Field('f1'),
                    Field.Virtual('vf2', f=lambda r: "virtual:"+r.A.f1, table_name='B'),
                    # FieldVirtual_WithDependancies('vf2', f=lambda r: "virtual:"+r.A.f1, table_name='B'),
                    Field('f3'),
                    Field('A_id', db.A)
                    )
    db.B.vf2.required_expressions=[db.A.f1]
    db.B.vf2.required_joins= [ db.A.on(db.B.A_id==db.A.id) ] # build_joins_chain(db.B, db.A)

    # TODO: test:
    # from plugin_AnySQLFORM import virtual_field
    # db.B.vf2 = virtual_field( 'vf2', f=lambda r: "virtual:"+r.A.f1, table_name='B',
    #                               required_expressions=[db.A.f1],
    #                               required_joins= [ db.A.on(db.B.A_id==db.A.id) ]
    # )

    db.A.truncate()
    db.B.truncate()

    for tablename in "AB":
        table = db[tablename]
        for nr in "1234":
            vals = {}
            for fieldname in table.fields[1:]:
                if fieldname.endswith('_id'):
                    vals[fieldname] = 5-int(nr)
                else:
                    vals [fieldname] = "%s%s:%s"% (tablename, fieldname, nr )

            table.insert( **vals )

    # generate more records in B with refs to A
    table = db.B
    import random
    for nr in range(5, 10):
        vals = {}
        for fieldname in table.fields[1:]:
            if fieldname=='A_id':
                vals[fieldname] = random.randint(1, 4)
            else:
                vals[fieldname] = "%s%s:%s" % (table._tablename, fieldname, nr)
        table.insert(**vals)

def test_02_virtual_field():
    init_tables_AB()

    # http://www.web2py.com/books/default/chapter/29/06/the-database-abstraction-layer#New-style-virtual-fields
    db.define_table('item',
                    Field('unit_price', 'double'),
                    Field('quantity', 'integer'))



    db.item.total_price = Field.Virtual('total_price',
                      lambda row: row.item.unit_price * row.item.quantity
                      # lambda row: row.A.f1 * row.item.quantity
                    , table_name='item' )

    db.item.truncate()
    for i in range(4):
        db.item.insert(unit_price=[5, 7, 2, 5][i], quantity=i)

    rows = db().select(db.item.ALL, db.item.quantity*2, join=db.A.on(db.A.id==1) )

    # return rows
    prices = [row.item.total_price for row in rows ]

    return str(prices)


def test_24_virtual_field():
    """
        Example:
        Table A: f1, f2
        Table B: f1, vf2(required_expressions: A.f1), f3, A_id

    columns = [ B.f1, B.vf2,  B.f3*3 ]
    -->
    virtual: [ B.vf2 ]
    selectable: [ B.f1, B.f3*3, A.f1 ]
    nonshown: [ A.f1 ]

    """

    init_tables_AB()
    columns = [db.B.f1, db.B.vf2, db.A.f2+'bla'] # Field, Field.Virtual, Expression

    # testGrid = False
    # if testGrid:
    #     db.A.f1.readable = False
    #     return SQLFORM.grid(db.B, fields=columns[:2]+[db.A.f1], left=db.A.on(db.A.id==db.B.id) )


    return select_with_virtuals(*columns) # same as  select_with_virtuals(db, *columns) #.as_json()

    # return select_with_virtuals(db, *columns, left=db.A.on(db.A.id==db.B.id)) #.as_json()
    # return db().select(db.demo.ALL)

def test_25_virtual_field_Represent():

    init_tables_AB()
    columns = [db.B.f1, db.B.vf2, db.A.f2 + 'bla']  # Field, Field.Virtual, Expression

    db.B.vf2.represent = lambda value: STRONG(value) # virtual field
    # need to inject representation in virutal field function directly
    vfunction = db.B.vf2.f  # to prevent recursive definition ;)
    db.B.vf2.f = lambda row: db.B.vf2.represent( vfunction(row)  )

    rows =  select_with_virtuals(*columns)

    db.B.f1.represent = lambda value: STRONG(value)  # ordinary field representation can be given even after selection

    rows.render()
    return rows



gt = None

def test_26_select_with_virtuals_Aggregated():
    """
        Example:
        Table A: f1, f2, vf3_agg(Aggregate list A)
        Table B: f1, vf2(required_expressions: A.f1), f3, A_id

    columns = [ B.f1, B.vf2,  B.f3*3 ]
    -->
    virtual: [ B.vf2 ]
    selectable: [ B.f1, B.f3*3, A.f1 ]
    nonshown: [ A.f1 ]

    """

    if not 'A' in db.tables or not 'B' in db.tables:
        init_tables_AB()


    # db.A.vf3_agg.f = lambda r: "ref %s" % r[db.B.id]

    # db.A.vf3_agg.required_expressions=[db.B.id]      # Postgre could be:, 'array_to_string(array_agg(B.id), ',')']
    # db.A.vf3_agg.required_joins= [ db.B.on(db.B.A_id==db.A.id) ] # build_joins_chain(db.B, db.A)
    db.A.vf3_agg.aggregate = dict(    groupby=db.A.id,
                                      select__kwargs=dict(left=[db.B.on(db.B.A_id == db.A.id)] ),
                                      # required_expressions=[db.B.id, db.B.f1],
                                      required_expressions=[db.B.id, db.B.f1],
                                      # f =   lambda row, group:  ', '.join( map(, map(lambda r: r[db.B.id], group )) )
                                      f =   lambda row, group:  ', '.join(  map(lambda r: "%(id)s %(f1)s"%r[db.B], group )  )
                                      , translator = gt
                                      )




    ########
    # Define cols
    columns = [db.A.id, db.A.vf3_agg ]; left=None # Field, Field.Virtual, Expression
    # columns = [db.A.id, db.B.id, db.A.vf3_agg ]; left = build_joins_chain(db.A, db.B) # Field, Field.Virtual, Expression

    # testGrid = False
    # if testGrid:
    #     db.A.f1.readable = False
    #     return SQLFORM.grid(db.B, fields=columns[:2]+[db.A.f1], left=db.A.on(db.A.id==db.B.id) )

    # rows = select_with_virtuals(
    rows = select_with_virtuals(
                    *columns
                    , query = db.A.id > 1
                    # , groupby=db.A.id
                    , translator = gt
                    , left=left
                    , orderby=db.A.id
                    , limitby=(0,5)
                          )

    return dict( rows=rows )



def test_26b_select_with_virtuals_Aggregated_withTranslator():
    global gt
    init_tables_AB()

    from plugin_GrandTranslator import GrandTranslator

    gt = GrandTranslator([db.B.f1], language_id=2)
    # gt.fields.append(db.B.f1)  # instruct translator to lookup B.f1

    return test_26_select_with_virtuals_Aggregated()

def test():
    cols = [db.auth_user.id, db.auth_user.first_name, db.auth_user.email, db.auth_group.role]
    selection = DalView(*cols,
                        # query=filter.query, having=filter.having,
                        left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                  )

    sql = selection.get_sql()
    print( "DBG SQL: ", sql )

    # data = SQLFORM.grid(search.query, fields=selected_fields, **kwargs )  # can toggle
    data = selection.execute()

    return dict(
            sql = sql,
            data = data,
            )



controller_dir = dir()

def index():
    from plugin_grand_helpers import make_menu
    make_menu(controller_dir)
    return dict(menu=MENU(response.menu), dbg=response.toolbar() )

def dbg():
    from plugin_AnySQLFORM import dbg
    return dbg()