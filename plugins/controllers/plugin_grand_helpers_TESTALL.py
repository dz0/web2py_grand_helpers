
def index():

    def components():
        for plugin in 'AnySQLFORM DalView GrandTranslator GrandRegister joins_builder'.split():
            yield A(plugin, _href=URL('plugin_' + plugin, 'index'))

    return dict(stuff=UL( [c for c in components() ] ))


def dbg():
    form = FORM(
        INPUT(_value='clear_session', _name='clear_session',  _type='submit' )
        , INPUT(_value='refresh', _name='refresh',  _type='submit' )
    )
    if request.vars.clear_session:
        session.clear()
        with open('/tmp/web2py_sql.log.html', 'w') as f:
            pass

    if request.vars.refresh:
        redirect(URL())

    try:
        with open('/tmp/web2py_sql.log.html') as f:
            sql_log_full=XML(f.read())
    except:
        sql_log_full=''

    return dict(dbg=response.toolbar(), form=form, session=session, sql_log_full=sql_log_full)


def tests_LOADed():

    def components():
        for plugin in 'AnySQLFORM DalView GrandTranslator GrandRegister'.split():
            yield LOAD('plugin_' + plugin, 'index', ajax=True)

    return dict(stuff=CAT( [c for c in components() ] ))