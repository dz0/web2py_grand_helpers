# -*- coding: utf-8 -*-

################## some auxilary functions 


# def get_databases(request):
    # dbs = {}
    # for (key, value) in globals().items():
        # cond = False
        # try:
            # cond = isinstance(value, GQLDB)
        # except:
            # cond = isinstance(value, SQLDB)
        # if cond:
            # dbs[key] = value
    # return dbs

# databases = get_databases(None)

#### 

# db = get_databases(request)[0]

# from plugin_AnySQLFORM import AnySQLFORM, FormField

def index():
    
    # for tablename in db.tables:
        # for field in db[tablename]:
    tables_HTML = SQLFORM.factory(
                 *[ Field( tablename, requires=IS_IN_SET( db[tablename], multiple=True ) )      for tablename in db.tables]
                )

    # may apply http://www.jqueryscript.net/form/jQuery-Plugin-To-Convert-Select-Options-To-Checkboxes-multicheck.html
    return dict(contents=tables_HTML)
