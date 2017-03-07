# -*- coding: utf-8 -*-

from pydal.objects import Field #, Row, Expression

from plugin_AnySQLFORM.AnySQLFORM import AnySQLFORM, FormField, get_expressions_from_formfields
from plugin_AnySQLFORM.AnySQLFORM import QuerySQLFORM, SearchField
from plugin_AnySQLFORM.GrandRegister import GrandRegister, DalView
from plugin_AnySQLFORM.GrandRegister import GrandTranslator, T_IS_IN_DB, GrandSQLFORM


# test fields
from  plugin_joins_builder.joins_builder import build_joins_chain

orphan_with_target = Field('orphan_name')
orphan_with_target.target_expression = db.auth_user.last_name + "bla"

TEST_FIELDS = [

    db.auth_user.first_name,
    db.auth_user.email,

    db.auth_user.id,
    db.auth_group.role,  # db.auth_group.description,

    FormField(db.auth_permission.table_name, requires=IS_IN_DB(db, db.auth_permission.table_name, multiple=True),
              comparison='equal'),
    SearchField(db.auth_permission.id,
                requires=IS_IN_DB(db, db.auth_permission, "%(name)s: %(table_name)s %(record_id)s")),

    # no_table items
    Field('user_id', type='reference auth_user'),
    #        FormField('user_id', type='reference auth_user'),

    Field('bla'),
    #        FormField( 'bla'),
    #         FormField( Field( 'expr_as_value' ), target_expression='string_value' ),  # orphan field with expression in kwargs
    #         FormField( 'direct_name', target_expression='direct_name_expr' ),  #  name with expression with expression in kwargs
    #         FormField( 5, name='str_expr_firstarg' ),  #  expression first -- even if it is just value
    orphan_with_target,
    # expression (as target)
    FormField(db.auth_user.first_name + db.auth_user.last_name, name='full_name', comparison='equal'),
    FormField(Field('pure_inputname_in_form'), name_extension='', prepend_tablename=False, target_expression='pure'),
]


def test_fields():
    return TEST_FIELDS

def test_20_queryform():
    fields = test_fields() 
    
    # db.auth_permission._format = "%(name)s %(table_name)s (%(id)s)"

    form = QuerySQLFORM( *fields )
    query_data = form.vars_as_Row()
    form.check_duplicate_fields_by_attrs('target_expression')
    filter = form.build_queries()
        
    return dict(
            filter = filter,
            form=form, 
            query_data = repr(query_data),
            # data = data,
            vars=form.vars,
            # vars_dict=repr(form.vars),
            )
    

def test_10_anyform():
    
    user_full_name = db.auth_user.first_name + db.auth_user.last_name
    
    # db.auth_permission._format = "%(name)s %(table_name)s (%(id)s)"
    
    fields = test_fields()
    
    form = AnySQLFORM( *fields )
    # form = SQLFORM.factory( *fields )

    data = form.vars_as_Row()
        
    return dict(
            form=form, 
            data_dict = repr(data),
            data = data,
            vars=form.vars,
            vars_dict=repr(form.vars),
            )


def test_22_dalview_search():

    fields = test_fields()
    fields[0].comparison = 'equal'
    
    form = QuerySQLFORM( *fields )

    form.check_duplicate_fields_by_attrs('target_expression')
    filter = form.build_queries()
    query_data = form.vars_as_Row()

    cols = get_expressions_from_formfields(fields)
    print "dbg cols", cols

    selection = DalView(*cols, query=filter.query, having=filter.having,
                  left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                  )

    sql = selection.get_sql()
    print( "DBG SQL: ", sql )

    # data = SQLFORM.grid(search.query, fields=selected_fields, **kwargs )  # can toggle
    data = selection.execute()

    return dict(
            sql = sql,
            filter = filter, # query and having
            form=form, 
            # query_data = repr(query_data),
            data = data,
            vars=form.vars,
            # vars_dict=repr(form.vars),
            )    

test_dalview_search = test_22_dalview_search
def test_27_dalview_search_VirtualField():
    f = TEST_FIELDS[0] = Field.Virtual( "virtual", label='Virtualus', f=lambda row: "bla")
    f.table = f._table = db.auth_user
    return test_dalview_search()



#################################################################################
#####################                      ###########################
#####################    Translator        ###########################
#####################                      ###########################
#################################################################################
gt = GrandTranslator(
    fields = [db.auth_user.first_name,   db.auth_group.role],   # we want to get tranlations only for first_name and role
    language_id=2
)
# def grandform( form_factory=SQLFORM.factory ):







def populate_fake_translations():

    # field = db.auth_user.first_name
    # field = db.auth_group.role

    print 'dbg select', db()._select( 'id', field )
    for r in db().select( 'id', field ):
        db.translation_field.insert(
            tablename = field._tablename,
            fieldname = field.name,
            rid = r['id'],
            language_id = 2,
            value = "EN_"+r[field]
        )


def test_30_grandtranslator_expressions():

    tests = [
        db.auth_user.first_name,  # Field
        db.auth_user.first_name + db.auth_user.last_name, # Flat Expression - 1 tranlsation
        db.auth_user.first_name + db.auth_group.role, # Flat Expression - 2 translations
        db.auth_user.first_name.contains('s'),  # Flat Query
        # Alias ?

        # structured / hierarchical / complex cases
        (db.auth_group.role+(db.auth_user.first_name + db.auth_user.last_name)), # complex Expression
        (db.auth_user.first_name.contains('s') | (db.auth_user.first_name=="John") ) & (db.auth_user.last_name=="BLA"),  # complex Query

        # list of expressions (any level depth/structure)
        tuple( [ ('list',  db.auth_group.role), db.auth_user.first_name.contains(['s', 'd']), ])
        ]

    def repr_t(t):  return map(str, [t.expr]+t.left  )

    results =  [ {expr: repr_t( gt.translate( expr ))}  for expr in tests]
    return dict(tests=results)

def test_31_grandtranslator_dalview():

    expr = db.auth_user.first_name + db.auth_user.last_name

    selection = DalView( expr ,  query=db.auth_user,
                         # left_join_chains=[[db.auth_user, db.auth_membership, db.auth_group]],
                         # left = build_joins_chain(db.auth_user, db.auth_membership, db.auth_group),
                         translator = gt
                  )

    sql = selection.get_sql(translate=False)
    sql_translated = selection.get_sql()

    return dict(
        sql=PRE(  sql  .replace("LEFT", "\nLEFT")),
        sql_translated=PRE(sql_translated  .replace("LEFT", "\nLEFT").replace("COALESCE", "\nCOALESCE")),
        data = selection.execute()
    )



def test_32_grandtranslator_dalview_search():

    fields = test_fields()

    column_fields= fields[:4]   # include expression

    expr_col = fields[-2]
    expr_col.comparison = 'equal' # # We will test Expression with IS_IN_SET widget
    search_fields= [ expr_col ] + column_fields

    form = GrandSQLFORM(*search_fields, translator=gt) # uses translator for validators

    # form.check_duplicate_fields_by_attrs('target_expression')
    filter = form.build_queries()
    query_data = form.vars_as_Row()


    # selection = DalView(*tfields, query=tquery, having=thaving, left_given = tleft,
    selection = DalView(*column_fields, query = filter.query, having = filter.having, left_given = (),
                        distinct = True,
                        left_join_chains=[[db.auth_user, db.auth_membership, db.auth_group, db.auth_permission]],
                        translator = gt
                        )

    # cols = get_expressions_from_formfields(fields)
    # print "dbg cols", cols
    # selection.fields = cols

    sql = selection.get_sql(translate=False)
    sql_translated = selection.get_sql()

    print("DBG SQL: ", sql_translated)

    # data = SQLFORM.grid(search.query, fields=selected_fields, **kwargs )  # can toggle
    data = selection.execute()

    return dict(
        sql=PRE(sql.replace("LEFT", "\nLEFT")),
        sql_translated=PRE(sql_translated.replace("LEFT", "\nLEFT").replace("COALESCE", "\nCOALESCE")),
        filter=filter,  # query and having
        form=form,
        # query_data = repr(query_data),
        data=data,
        vars=form.vars,
        # vars_dict=repr(form.vars),
    )

    """    """



#################################################################################
#####################                      ###########################
#####################    GRAND REGISTER        ###########################
#####################                      ###########################
#################################################################################

def test_41_grandregister_form_and_ajax_records(  ):
    search_fields = test_fields()
    cols = get_expressions_from_formfields(search_fields )

    register = GrandRegister(cols,
                             cid = 'w2ui_test', # w2ui
                             data_name = 'test grand',
                             table_name = 'test_grand',
                             search_fields = search_fields ,
                             left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                             # , response_view = None
                             , translator = gt
                             , use_grand_search_form = False
                             )

    # response.view = ...
    if request.vars._grid:
        rows = register.w2ui_grid_records()

        # return BEAUTIFY(  [ filter, rows ]  )  # for testing

        if request.vars._grid_dbg:
            def as_htmltable(rows, colnames):
                from gluon.html import TABLE
                return TABLE([colnames] + [[row[col] for col in colnames] for row in rows])
            rows = as_htmltable(rows, [FormField(col).name for col in register.columns]) # for testing

        # from gluon.serializers import json
        # return json(dict(status='success', records = rows ))
        response.view = "generic.json"
        return dict(status='success', records = rows )  # JSON

        # return DIV( filter, register.records_w2ui() )
        # return dict(records=register.records_w2ui())

    else:
        # response.view = "plugin_w2ui_grid/w2ui_grid.html"
        # register.search_form.      add_button( 'grid', URL(vars=dict(grid=True)))

        result = register.form()

        # for debug purposes:
        # tablename = register.search_form.table._tablename
        ajax_url = "javascript:ajax('%s', %s, 'grid_records'); " % (
                                                URL(vars=dict(_grid=True, _grid_dbg=True), extension=None)  ,
                                                [f.name for f in register.search_fields]
                   )

        ajax_link = A('ajax load records', _href=ajax_url, _id="ajax_loader_link")
        ajax_result_target = DIV( BEAUTIFY(register.w2ui_grid_records() ), _id='grid_records')
        # register.search_form.      add_button( 'ajax load records', ajax_url )
        # result['ajax_records']=
        # result['ats']=ajax_result_target

        # register.search_form[0].insert(0, ajax_link)
        result['form'] = CAT(ajax_result_target , ajax_link, result['form'] )


        return result




# SWITCH to use searchform with SOLIDFORM.factory  and fast_filters
use_grand_search_form=True # default is True (and needs test_app)

def test_49_TODO_grandregister_autocomplete_breaks_fastfilters_for_same_field():
    pass
    #TODO

def test_47_grandregister():
    search_fields = test_fields() [:4]
    cols = get_expressions_from_formfields(search_fields )

    from plugin_AnySQLFORM.GrandRegister import create_fast_filters, SearchField
    sf = search_fields[0]
    # fast_filters = create_fast_filters(  sf ) # without translations  # TODO test more thoroughly
    translated_values = DalView( sf, distinct=True, translator = gt ).execute().column( sf )
    fast_filters = create_fast_filters( sf, values=translated_values )  # TODO test more thoroughly

    if use_grand_search_form:
        # fast filters
        # f = search_fields[0]
        # sf = SearchField( f )
        # fast_filters = [  {'label': T('core__all'), 'selected': True, 'data': {}},  ]
        # for row in db(f).select():
        #     fast_filters.append({'label': row[f.name], 'data': {sf.name: row[f.name]}})


        # for SOLIDFORM
        a, b, c, d =  search_fields
        search_fields_structured = [   [a, b],    [c, d]    ]

        kwargs = dict( search_fields=search_fields_structured  #,  filters=fast_filters
                       #, response_view = "" # "plugin_AnySQLFORM/w2ui_grid.html"
                     )
    else:
        kwargs = dict( search_fields=search_fields,   use_grand_search_form=False        )


    register = GrandRegister(cols,
                             cid='w2ui_test', # w2ui
                             table_name = 'test_grand',

                             left_join_chains=[[ db.auth_user, db.auth_membership, db.auth_group, db.auth_permission ]]
                             # , response_view = None
                             , translator=gt

                              # search_fields=search_fields, use_grand_search_form=use_grand_search_form,
                              # search_fields=search_fields,  use_grand_search_form=False  # default tries SOLIDFORM
                              # search_fields = search_fields_structured,  # for SOLIDFORM         # search_fields = [ search_fields ],
                             , filters = fast_filters
                             , formstyle =  None
                             , **kwargs  # SEARCH FIELDS, etc
                             )
    register.render()

test_grandregister = test_47_grandregister
def test_42_grandregister_with_just_SQLFORM():
    global use_grand_search_form
    use_grand_search_form = False
    test_grandregister()


# @auth.requires_permission('list', 'subject_subject')
def test_60_granderp_subjects():
        from lib.branch import allowed_managers, allowed_subjects_query, is_branch_allowed

        response.subtitle = T('subject_subject__list_form')
        response.files.append(URL('static', 'subject/js/subjects.js'))

        cid = 'subjects'
        filters = [{'label': T('core__all'), 'selected': True, 'data': {}}]
        for c, l in SUBJECT_CLASSES:
            filters.append({'label': l, 'data': {'classes[]': c}})
        db.subject_subject.type.default = None

        search_fields =  [
                [db.subject_subject.title,
                 Field('manager_ids[]', label=T('subject_subject__manager'),
                       requires=IS_IN_SET(allowed_managers(db, auth), multiple=True))],

                [Field('classes[]', requires=IS_IN_SET(SUBJECT_CLASSES, multiple=True),
                       label=T('subject_subject__classes')),
                 db.subject_subject.rating_id],

                [db.subject_subject.type, db.address_address.country_id],
                [db.subject_subject.branch_id, db.address_address.city],
                [db.subject_subject.campaign_id, Field('street', label=T('subject_subject__address'))],
                [db.subject_subject.activity_id, db.contact_contact.phone],
            ]


        f = db.subject_subject.contact_id
        f.tablename = f._tablename = "subject_subject"
        cols = [
            db.subject_subject.title,
            db.subject_subject.contact_id, # Virtual
            # db.subject_subject.address_id, # Virtual


            db.subject_subject.activity_id,
            # Field('subject_subject__main_note', label=T('subject_subject__main_note')),
            db.address_address.country_id,
            # db.subject_subject.website,
            db.subject_subject.pay_term,
            # db.subject_subject.credit_bank_id,
        ]

        register = GrandRegister(cols,
                                 cid=cid,
                                 table_name='subject_subject',

                                 # left_join_chains=[
                                 #     [db.auth_user, db.auth_membership, db.auth_group, db.auth_permission]
                                 # ],
                                 search_fields = search_fields,
                                 filters=filters, # fast filters
                                 # translator=gt

                                formstyle =  None #'divs' if IS_MOBILE else None,
                                # _class = 'mobile_sqlform' if IS_MOBILE else None,
                                 )
        register.render()
        # form = search_form(
        #     cid,
        #     *search_fields,
        #     # hidden={'subjects_autocomplete_title': URL('subject', 'autocomplete_subject_title.json'),
        #     #         'subjects_autocomplete_phone': URL('subject', 'autocomplete_subject_phone.json'),
        #     #         'subjects_autocomplete_street': URL('subject', 'autocomplete_subject_street.json'),
        #     #         'subjects_autocomplete_city': URL('subject', 'autocomplete_subject_city.json')},
        #     table_name='subject_subject',
        #     # formstyle='divs' if IS_MOBILE else None,
        #     filters=filters
        # )
        #
        # return {'cid': cid, 'form': form}


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

def test_80_postgre_distinct():
    sel = DalView(db.auth_user.first_name, distinct=True, translator=gt) # orderby=db.auth_user.first_name
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
    db.define_table('A',  Field('f1'),  Field('f2'),  Field.Virtual('vf3', f=lambda r:None, table_name='A')  )
    db.define_table('B',
                    Field('f1'),
                    Field.Virtual('vf2', f=lambda r: "virtual:"+r.A.f1, table_name='B'),
                    # FieldVirtual_WithDependancies('vf2', f=lambda r: "virtual:"+r.A.f1, table_name='B'),
                    Field('f3'),
                    Field('A_id', db.A)
                    )
    db.B.vf2.required_expressions=[db.A.f1]
    db.B.vf2.required_joins= [ db.A.on(db.B.A_id==db.A.id) ] # build_joins_chain(db.B, db.A)

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



class FieldVirtual_WithDependancies(Field.Virtual):
    def __init__(self, name, f=None, ftype='string', label=None, table_name=None,
                 required_expressions=[],  required_joins=[]):
        Field.Virtual.__init__(self, name, f, ftype, label, table_name)
        self.required_expressions = required_expressions
        self.required_joins = required_joins


class FieldVirtual_Aggregate(Field.Virtual):
    pass
    # def __init__(self, name, f=None, ftype='string', label=None, table_name=None):
    #     Field.Virtual.__init__(self, name, f, ftype, label, table_name)

def select_with_virtuals(dbset, *columns,  **kwargs):
    """columns can be instance of Expression, Field, Field.Virtual

    acts similary as SQLFORM.grid, but returns Rows object.
    in the result: records are remapped according to columns,
    but rawrows and colnames stay as they are in the select...

    nonshown can indicate which columns to select (for virtuals) but exclude from result
    ps.: in SQLFORM.grid this is done with readable.False

    Example:
        Table A: f1, f2
        Table B: f1, vf2(required_expressions: A.f1), f3

    columns = [ B.f1, B.vf2,  B.f3*3 ]
    -->
    virtual: [ B.vf2 ]
    selectable: [ B.f1, B.f3*3, A.f1 ]
    nonshown: [ A.f1 ]

    """


    selectable = []
    virtual = [];    join_chains = []
    nonshown = kwargs.pop('nonshown', [])[:]  # used by virtual, but not included in result



    ### init: assign what is where
    for col in columns:
        if isinstance(col, Field.Virtual):
            if col not in virtual:
                virtual.append( col )

                # look for dependances
                for required_expr in getattr(col, 'required_expressions', []):
                    if required_expr not in nonshown:
                        nonshown.append( required_expr )
                required_joins = getattr(col, 'required_joins', [])
                if required_joins:
                    join_chains.extend(  required_joins )
        else:
            selectable.append( col )

    # make sure nonshown items doesn't appear in columns
    nonshown = set(nonshown).difference(columns)


    # more stuff to nonshow:    #  if not f.readable
    if kwargs.get('nonreadable_as_nonshown', False):
        nonreadable =   set ( f for f in columns if not f.readable)
        nonshown .update( nonreadable )
        columns = [  f for f in columns if f.readable ] # overrides original columns!

    nonshown = list(nonshown)

    # do SELECT
    if required_joins:
        kwargs.setdefault('left', []).extend(  required_joins )
    rows = dbset().select(*(selectable+nonshown), **kwargs)

    # remap to resulting fields
    tmp_compact, rows.compact = rows.compact, False
    for row in rows:
        # add virtual fields
        # this expects virtualfields to have tablename  (or could use tablenames_4_virtualfields)
        # https://github.com/web2py/web2py/blob/master/gluon/sqlhtml.py#L2862
        for field in virtual:
            if isinstance(field, Field.Virtual) and field.tablename in row:

                    if hasattr(field, 'f_aggregate'):
                        field.f = lambda r: field.f_aggregate(r, context_rows=rows)

                        row[field.tablename][field.name] = field.f_aggregate(row, context_rows=rows)
                        # TODO: remove else
                    else:
                    # try:
                        # fast path, works for joins
                        value = row[field.tablename][field.name]
                        row[field.tablename][field.name] = value # this replaces call with value
                    # except KeyError:
                    #     value = dbset.db[field.tablename] [row[field.tablename][field_id]]  [field.name]


        # remove nonshown fields/expressions
        for field in nonshown:
            del row[field.tablename][field.name]
            # if len(row[field.tablename]) == 0:
            if not row[field.tablename]:
                del row[field.tablename]

    rows.compact = tmp_compact

    # rows.colnames = rows.colnames[:]
    # for field in nonshown:
    #     rows.colnames.remove(str(field))
    rows.rawcolnames = rows.colnames
    rows.colnames = [str(col) for col in columns]


    return rows

    def tablenames_4_virtualfields():
        db = dbset
        left = kwargs.get('left', [])
        fields = columns

        # taken from SQLFORM.grid https://github.com/web2py/web2py/blob/master/gluon/sqlhtml.py#L2326
        tablenames = db._adapter.tables(dbset.query)
        if left is not None:
            if not isinstance(left, (list, tuple)):
                left = [left]
            for join in left:
                tablenames += db._adapter.tables(join)
        tables = [db[tablename] for tablename in tablenames]
        if fields:
            # add missing tablename to virtual fields
            for table in tables:
                for k, f in table.iteritems():
                    if isinstance(f, Field.Virtual):
                        f.tablename = table._tablename
            columns = [f for f in fields if f.tablename in tablenames]



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

    # return select_with_virtuals(db(db.B))

    return select_with_virtuals(db, *columns) #.as_json()
    # return select_with_virtuals(db, *columns, left=db.A.on(db.A.id==db.B.id)) #.as_json()
    # return db().select(db.demo.ALL)

def test_25_virtual_field_Represent_TODO():
    rows = test_24_virtual_field()
    rows.render()
    return rows

def test_26_virtual_field_Aggregate():
    """
        Example:
        Table A: f1, f2, vf3(Aggregate list A)
        Table B: f1, vf2(required_expressions: A.f1), f3, A_id

    columns = [ B.f1, B.vf2,  B.f3*3 ]
    -->
    virtual: [ B.vf2 ]
    selectable: [ B.f1, B.f3*3, A.f1 ]
    nonshown: [ A.f1 ]

    """

    init_tables_AB()

    db.A.vf3.required_expressions=[db.B.id] # Postgre could be:, 'array_to_string(array_agg(B.id), ',')']
    db.A.vf3.required_joins= [ db.B.on(db.B.A_id==db.A.id) ] # build_joins_chain(db.B, db.A)

    def agg_list(row, context_rows=None):
        """context_rows is Rows object, which has the stuff to get """
        my_id = row[db.A][db.A.id]
        if context_rows is not None:
            # ids = context_rows.column(db.B.id)
            if hasattr(context_rows, 'grouped_Avf3'):  # if cached
                grouped = context_rows.grouped_Avf3
            else:
                grouped = context_rows.group_by_value(db.A.id)
                context_rows.grouped_Avf3 = grouped # cache
        else:
            "there is nowhere to cache? - do fresh request..."
            # rows = select(... belongs...)
            # rows.compact = False
            # grouped = context_rows.group_by_value(db.A.id)
            # pass


        agg = [r[db.B][db.B.id]  for r in  grouped[my_id]  ]
        agg_concat = ', '.join( map(str, agg ) )
        return agg_concat


    db.A.vf3.f_aggregate = agg_list

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

    ########
    # Define cols
    columns = [db.A.id, db.B.id, db.A.vf3 , ] # Field, Field.Virtual, Expression

    # testGrid = False
    # if testGrid:
    #     db.A.f1.readable = False
    #     return SQLFORM.grid(db.B, fields=columns[:2]+[db.A.f1], left=db.A.on(db.A.id==db.B.id) )

    return select_with_virtuals(db, *columns
                                # , groupby=db.A.id
                                , orderby=db.A.id
                                )



controller_dir = dir()
def menu4tests():
    test_functions = [x for x in controller_dir if x.startswith('test') and x!='tester']
    response.menu = [('TESTS', False, '',
                        [
                            (f, f==request.function, URL(f) )
                            for f in test_functions
                        ]
                     ),
                     ('populate auth tables', False, URL('populate_fake_auth') ),
                    ]
    return response.menu



def index():
    menu4tests()
    return dict(menu=MENU(response.menu), dbg=response.toolbar())