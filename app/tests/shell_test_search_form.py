# iskvietimas:
# python web2py.py -S grids/test_search_form/index -M 


def index():
        
    
    # In [44]: f1 = SQLFORM.factory( db.auth_user.email, db.auth_user.last_name )

    # In [45]: f2 = SQLFORM.factory( db.auth_user.email )

    # In [46]: f3 = SQLFORM.factory( db.auth_user.email, db.auth_user.email )

    # In [47]: print str(f1).replace('<input', "\n<INPUT")
    # In [48]: print str(f2).replace('<input', "\n<INPUT")
    # In [49]: print str(f3).replace('<input', "\n<INPUT")


    _from = db.auth_user.email.clone()        
    # Out[53]: <pydal.objects.Field at 0x7f18931581d0>
    # In [54]: str(_from)
    # Out[54]: 'auth_user.email'

    _from.name = "from"

    # In [56]: str(_from)
    # Out[56]: 'auth_user.from'


    _to = db.auth_user.email.clone()     

    _to.name = "to"

    f4 = SQLFORM.factory( _from, _to )

    # In [60]: print str(f4).replace('<input', "\n<INPUT")
    # <form action="#" class="form-horizontal" enctype="multipart/form-data" method="post"><div class="form-group" id="no_table_from__row"><label class="control-label col-sm-3" for="no_table_from" id="no_table_from__label">E-mail</label><div class="col-sm-9">
    # <INPUT class="form-control string" id="no_table_from" name="from" type="text" value="" /><span class="help-block"></span></div></div><div class="form-group" id="no_table_to__row"><label class="control-label col-sm-3" for="no_table_to" id="no_table_to__label">E-mail</label><div class="col-sm-9">
    # <INPUT class="form-control string" id="no_table_to" name="to" type="text" value="" /><span class="help-block"></span></div></div><div class="form-group" id="submit_record__row"><div class="col-sm-9 col-sm-offset-3">
    # <INPUT class="btn btn-primary" type="submit" value="Submit" /></div></div></form>

    return dict( form=f4, html=str(f4).replace('<input', "\n<INPUT") ) 

"""
Hi,

I want to construct search forms, and sometimes I need to have several same fields in them..


For simplified example, let's say, I need 2 email fields:
 - to search messages by "from"
 - to search messages by "to"

SQLFORM.factory( db.auth_user.email, db.auth_user.email )

but it gives me just one field without any warning. Which would be ok in most cases.
But I need 2 fields :)

I mangled a bit with "clone", but didn't succed:

        from = db.auth_user.email.clone()        
        from.name = "from"

        to = db.auth_user.email.clone()     
        to.name = "to"


f4 = SQLFORM.factory( _from, _to )
print str(f4).replace('<input', "\n<INPUT")

"""
