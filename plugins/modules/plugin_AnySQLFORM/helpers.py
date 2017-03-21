def get_fields_from_table_format(format_str):  
    """
    >>> get_fields_from_table_format( '%(first_name)s %(last_name)s' )
    ['first_name',  'last_name']
    """
    import re
    regex = r"\%\((.+?)\)s"
    results = re.findall(regex, format_str)
    return results


def save_DAL_log(file='/tmp/web2py_sql.log.html'):
    from gluon.html import XML, TABLE, TR, PRE, BEAUTIFY
    from gluon.dal import DAL
    dbstats = []
    dbtables = {}
    infos = DAL.get_instances()
    for k, v in infos.iteritems():
        dbstats.append(TABLE(*[TR(PRE(row[0]), '%.2fms' % (row[1]*1000))
                               for row in v['dbstats']]))
        # dbtables[k] = dict(defined=v['dbtables']['defined'] or '[no defined tables]',
                           # lazy=v['dbtables']['lazy'] or '[no lazy tables]')
    
    with open(file, 'w') as f:
        f.write( str(BEAUTIFY(dbstats)) )
    
