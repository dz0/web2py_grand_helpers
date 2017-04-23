# -*- coding: utf-8 -*-

from gluon import current
import copy

def join_dicts(d1, d2, result_as_new=True, allow_overlaping_keys=False):

    if not allow_overlaping_keys:
        overlaping_keys = set(d1.keys()) & set(d2.keys())
        if overlaping_keys:
            raise RuntimeError("join_dicts got overlaping_keys: %r" % overlaping_keys)

    if result_as_new:
        result = copy.copy(d1)
    else:
        result = d1

    for key, val in d2.items():
        if not key in d1:
            result[key] = val
    return result

def update_dict_override_empty(d1, d2):
    raise RuntimeError("Depreciated?")
    for key, val in d2.items():
        if key in d1 and d1[key]: # if already has some important value
            continue
        d1[key] = val
    return d1

def get_fields_from_table_format(format_str):
    """
    >>> get_fields_from_table_format( '%(first_name)s %(last_name)s' )
    ['first_name',  'last_name']
    """
    import re
    regex = r"\%\((.+?)\)s"
    results = re.findall(regex, format_str)
    return results


def fields_are_equal(a, b):
    "workaround of quirk of Field/Expression, as explained https://groups.google.com/d/msg/web2py/LvR7mGFX7UQ/VN5H6AS-AQAJ"
    return str(a) == str(b)

def extend_with_unique(A, B):
    """for Expression/Fields:  extends list A with distinct items from B, which were not present in A
    for Expression/Fields instances one needs to use str(..) asotherwise  overriden _eq_ understands different fields as same.. :/
    """
    for b in B:
        if  str(b) not in map(str, A):
            A.append(b)

def prepend_with_unique(A, B):
    for b in B:
        if  str(b) not in map(str, A):
            A.insert(0, b)

def append_unique(A, b):
    "for fields/expressions"
    if str(b) not in map(str, A):
        A.append(b)


def is_aggregate( expr ):
    if isinstance(expr, str):
        return # TODO : regexp to see if it has SUM AVERAGE COUNT...

    if not hasattr(expr, 'op'):
        return

    db = getattr(expr, 'db', current.db)  # target expression might be str type
    return expr.op in [db._adapter.AGGREGATE, db._adapter.COUNT]
                # [db._adapter.dialect.AGGREGATE, db._adapter.dialect.COUNT]:  # for newer pydal... untested

def is_reference(field):
    try:
        ref_indicator = field.type.split()[0]
        return ref_indicator in ['reference', 'list:reference']
    except:
        pass

def is_id(field):
    return str(field)==str(field.table._id) or is_reference(field)




def get_expressions_from_formfields( formfields, include_orphans=False ):
    from plugin_AnySQLFORM import  FormField
    from pydal import Field
    from pydal.objects import Expression
    # result = []
    # for f in formfields_flat:
    #     if isinstance(f, FormField):
    #         result.append( f.target_expression )
    #     else:
    #         result.append( f )
    result =  [f.target_expression if isinstance(f, FormField) else f     for f in formfields ]
    if not include_orphans:
        result = [expr for expr in result
                        if isinstance(expr, Field) and getattr(expr, 'tablename', 'no_table') != 'no_table'
                        or type(expr) is Expression
                  ]
    return result

def get_distinct(target):
    db = current.db

    # return True
    # return target
    import pydal
    # TODO FIXME chekc if this works
    if isinstance(db._adapter , pydal.adapters.sqlite.SQLiteAdapter ):
        return True
    elif isinstance(db._adapter, pydal.adapters.postgres.PostgreSQLAdapter):
        return target
    else:
        print "WARNING: not sure what should be passed as 'distinct' arg for adapter %r" % db._adapter
        return True


#
def force_refs_represent_ordinary_int(rows):
    db = current.db
    rows.compact=False
    row = rows[0]
    for table in row:  # TODO: not tested
        if table != '_extra':

            for f in row[table]:

                if f in db[table].fields and  is_reference( db[table][f] ):
                    db[table][f].represent = None

            # for f, val in row[table].items():
            #     from pydal.helpers.classes import Reference
            #     if isinstance( val, Reference):
            #         db[table][f].represent = None




from gluon.html import XML, TABLE, TR, PRE, BEAUTIFY, STYLE, CAT, DIV, B
from gluon.dal import DAL


def tidy_SQL(sql, wrap_PRE=True):
    for w in 'from left inner where'.upper().split():
        sql = sql.replace(w, '\n' + w)
    sql = sql.replace('AND', '\n      AND')

    if wrap_PRE: sql = PRE(sql)
    else: sql = "\n %s \n" % sql

    return sql

def sql_log_find_last_pos( item ):  # to find item from end (if it was not trimmed depending on TIMINGSSIZE)
    log = get_sql_log()

    for nr in range(len(log)-1, -1, -1):
        if item is log[nr]:
            return nr

    return -1

def get_sql_log(start=0, end=None):
    # sqls = None
    # for conn, info in DAL.get_instances().items():
    #     if sqls  is None:
    #         sqls = info['dbstats']
    #     else:
    #         sqls.extend( info['dbstats'] )

    db = current.db
    sqls = db._timings

    if start == 0 and  end is None:
        return sqls

    else:
        if end is None:
            end= len(sqls)
        return sqls[start:end]


def sql_log_format(sql_log):
    return CAT(

        TABLE(
            TR(B("TOTAL TIME: "), B('%.2fms' % sum([row[1] * 1000 for row in sql_log]))),
            *[TR( nr+1, tidy_SQL(row[0]),
                    '%.2fms' % (row[1]*1000)
                    )
                    for nr, row in enumerate(sql_log)
                ]

              )

        , sql_log_style
    )

sql_log_style =   STYLE("""
            table {width:95%}

            pre {
                padding: 5px;
                line-height: 1.2em;
            xwidth: 80%;
            display: inline-block;
            word-wrap: break-word;
            word-break: break-all;
                white-space: pre-wrap;
                border : 1px silver solid;
            }
            """)

def set_TIMINGSSIZE(n):
    import pydal.adapters.base as _
    _.TIMINGSSIZE = n

def save_DAL_log(file='/tmp/web2py_sql.log.html', mode='w', flush=False):
    # dbstats = []
    # dbtables = {}
    # infos = DAL.get_instances()
    # for k, v in infos.iteritems():
    #     dbstats.append(  sql_log_format( v['dbstats']  ) )
    #     # dbtables[k] = dict(defined=v['dbtables']['defined'] or '[no defined tables]',
    #                        # lazy=v['dbtables']['lazy'] or '[no lazy tables]')

    timings = current.db._timings

    dbstats = sql_log_format( timings )
    with open(file, mode) as f:
        f.write( str(  dbstats  ) )
        f.write(str( sql_log_style ) );

    if flush:
        del timings[:]






##################
def represent_joins( joins ):
    # def ON(self, first, second):
    #     table_rname = self.table_alias(first)
    #     if use_common_filters(second):
    #         second = self.common_filter(second,[first._tablename])
    #     return ('%s ON %s') % (self.expand(table_rname), self.expand(second))

    reprs = []
    db = current.db
    for expr in joins:
        reprs.append( db._adapter.ON( expr.first, expr.second ))

    return reprs
###################

def make_menu(controller_dir, menuname=None, append=False):
    from gluon.html import URL

    test_functions = [x for x in controller_dir if x.startswith('test') and x!='tester']
    test_functions.sort()

    menu = [
                     (menuname or 'TESTS', False, '',
                        [
                            (f, f==current.request.function, URL(f) )
                            for f in test_functions
                        ]
                     ),
                     # ('populate auth tables', False, URL('populate_fake_auth') ),
                    ]
    if append and current.response.menu:
        current.response.menu.extend( menu )
    else:
        current.response.menu = menu
    return menu



# FOR TEST controllers

TEST_FIELDS = None

def test_fields():
    if globals().get('TEST_FIELDS') is None:
        db = current.db
        from pydal import Field
        from plugin_AnySQLFORM import FormField, SearchField
        from gluon.validators import IS_IN_DB
        global TEST_FIELDS
        orphan_with_target = Field('orphan_name')
        orphan_with_target.target_expression = db.auth_user.last_name + "bla"

        TEST_FIELDS = [

            db.auth_user.first_name,
            db.auth_user.email,

            db.auth_user.id,
            db.auth_group.role,  # db.auth_group.description,

            FormField(db.auth_permission.table_name,
                      requires=IS_IN_DB(db, db.auth_permission.table_name, multiple=True),
                      comparison='equal'),
            SearchField(db.auth_permission.id,
                        requires=IS_IN_DB(db, db.auth_permission, "%(name)s: %(table_name)s %(record_id)s") # possibly overriden..
                        ),

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
            FormField(Field('pure_inputname_in_form'), name_extension='', prepend_tablename=False,
                      target_expression='pure'),
        ]
    return TEST_FIELDS


# test_fields()