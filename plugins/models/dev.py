# forces/tries modules reload on change

from gluon.custom_import import track_changes
track_changes(True)


from gluon import current

# Toggles limitby in DalView... for DBG purposes
current.dev_limitby = 0, 20
current.DBG = True

# fake admin
if False:
    auth.has_permission= lambda *args, **kwargs: True
    from gluon.storage import Storage
    # auth.user= Storage( language_id = 1 )
    auth.user=db.auth_user(1) # Storage( language_id = 1 )
