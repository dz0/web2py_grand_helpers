# -*- coding: utf-8 -*-

from gluon import current

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

def append_unique(A, b):
    "for fields/expressions"
    if str(b) not in map(str, A):
        A.append(b)


def is_aggregate( expr ):
    if isinstance(expr, str):
        return # TODO : regexp to see if it has SUM AVERAGE COUNT...

    db = getattr(expr, 'db', current.db)  # target expression might be str type
    return expr.op in [db._adapter.AGGREGATE, db._adapter.COUNT]
                # [db._adapter.dialect.AGGREGATE, db._adapter.dialect.COUNT]:  # for newer pydal... untested

def is_reference(field):
    ref_indicator = field.type.split()[0]
    return ref_indicator in ['reference', 'list:reference']

def is_id(field):
    return str(field)==str(field.table._id) or is_reference(field)




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




from gluon.html import XML, TABLE, TR, PRE, BEAUTIFY, STYLE, CAT, DIV
from gluon.dal import DAL


def tidy_SQL(sql, wrap_PRE=True):
    for w in 'from left inner where'.upper().split():
        sql = sql.replace(w, '\n' + w)
    sql = sql.replace('AND', '\n      AND')

    if wrap_PRE: sql = PRE(sql)
    else: sql = "\n %s \n" % sql

    return sql

def save_DAL_log(file='/tmp/web2py_sql.log.html'):
    dbstats = []
    dbtables = {}
    infos = DAL.get_instances()
    for k, v in infos.iteritems():
        dbstats.append(TABLE(*[TR( tidy_SQL(row[0]), '%.2fms' % (row[1]*1000))
                               for row in v['dbstats']]))
        # dbtables[k] = dict(defined=v['dbtables']['defined'] or '[no defined tables]',
                           # lazy=v['dbtables']['lazy'] or '[no lazy tables]')
    
    with open(file, 'w') as f:
        f.write( str(DIV(*dbstats)) )
        f.write(str( STYLE("""
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
        """)) );
    
