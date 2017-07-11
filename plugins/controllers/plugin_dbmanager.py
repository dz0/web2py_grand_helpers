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

def index():
    
    # for tablename in db.tables:
        # for field in db[tablename]:
    tables_HTML = []
    for tablename in db.tables:
        
        fields_HTML = SQLFORM.factory( *[Field(f.name, type='boolean') for f in db[tablename] ] )
        
        tables_HTML.append( DIV(tablename, fields_HTML)    )

    
    result = CAT( UL(tables_HTML) , STYLE("li {float:left; padding:30px}; ") )
    return result
