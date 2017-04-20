# -*- coding: utf-8 -*-

from pydal import Field #, Row, Expression

from plugin_DalView import DalView

from plugin_AnySQLFORM import AnySQLFORM, FormField
from plugin_AnySQLFORM import SearchSQLFORM, SearchField

from plugin_grand_helpers import save_DAL_log, test_fields, TEST_FIELDS, get_expressions_from_formfields

# test fields
# from  plugin_joins_builder import build_joins_chain


def test_10_anyform():
    user_full_name = db.auth_user.first_name + db.auth_user.last_name

    # db.auth_permission._format = "%(name)s %(table_name)s (%(id)s)"

    fields = test_fields()
    # print 'dbg test_fields', fields

    form = AnySQLFORM(*fields)
    # form = SQLFORM.factory( *fields )

    data = form.vars_as_Row()

    return dict(
        form=form,
        data_dict=repr(data),
        data=data,
        vars=form.vars,
        vars_dict=repr(form.vars),
    )


def test_20_SearchFORM():
    fields = test_fields()

    # db.auth_permission._format = "%(name)s %(table_name)s (%(id)s)"

    form = SearchSQLFORM(*fields)
    query_data = form.vars_as_Row()
    form.check_duplicate_fields_by_attrs('target_expression')
    filter = form.build_queries()

    return dict(
        filter=filter,
        form=form,
        query_data=repr(query_data),
        # data = data,
        vars=form.vars,
        # vars_dict=repr(form.vars),
    )


def test_22_dalview_search():
    fields = test_fields()
    fields[0].comparison = 'equal'

    form = SearchSQLFORM(*fields)

    form.check_duplicate_fields_by_attrs('target_expression')
    filter = form.build_queries()
    query_data = form.vars_as_Row()

    cols = get_expressions_from_formfields(fields, include_orphans=False)  # for 27 should change to True
    print "dbg cols", cols

    selection = DalView(*cols, query=filter.query, having=filter.having,
                        left_join_chains=[[db.auth_user, db.auth_membership, db.auth_group, db.auth_permission]]
                        )

    sql = selection.get_sql()
    print("DBG SQL: ", sql)

    # data = SQLFORM.grid(search.query, fields=selected_fields, **kwargs )  # can toggle
    data = selection.execute()

    return dict(
        sql=sql,
        filter=filter,  # query and having
        form=form,
        # query_data = repr(query_data),
        data=data,
        vars=form.vars,
        # vars_dict=repr(form.vars),
    )


test_dalview_search = test_22_dalview_search


def test_23_virtual_field_dalview_search_TODO_orNotTODO():
    vf = db.auth_user.vf = TEST_FIELDS[0] = Field.Virtual("virtual", label='Virtualus', f=lambda row: "bla",
                                                          table_name='auth_user')
    vf.table = vf._table = db.auth_user

    return test_dalview_search()


def test_25_SearchField_requires_multiple():

    form = SearchSQLFORM(
         # SearchField(db.auth_user.email, multiple=True, override_widget=True)
        # Note: multiple=True forces override_widget=True
         SearchField(db.auth_user.email, multiple=True)
        , SearchField(db.auth_membership.user_id, multiple=True)
        , SearchField(db.auth_user.id, multiple=True)
        # , keepvalues = True
        )

    return dict(form=form,
                vars=request.vars
                , queries=form.build_queries()
                )

def test_70_AnySQLFORM_edit():
    table = db.auth_user
    item = table(2)

    fields = [table[f] for f in table.fields]

    for f in fields:
        # f.default = item[f]   # prepopulating form using "default" value  -- doesn't work with .process()
        f.override_validator = False

    form = AnySQLFORM(*fields)

    # return form  # plain from...

    # prepopulating form using form.vars
    # http://web2py.com/books/default/chapter/29/07/forms-and-validators#Pre-populating-the-form
    for f in form.formfields:
        form.vars[f.name] = item[f.target_expression]

    form.process()
    return dict( form=form, vars=form.vars_as_Row() )

import datetime
# from helpers import  represent_datetime # action_button, expandable_section,







controller_dir = dir()
def index():
    from plugin_grand_helpers import make_menu
    make_menu(controller_dir)
    return dict(menu=MENU(response.menu), dbg=response.toolbar() )

