# forces/tries modules reload on change

from gluon.custom_import import track_changes
track_changes(True)

from gluon import current

current.DBG = True
current.dev_limitby = 0, 10  # sets limitby in DalView... for DBG purposes

############# DAL/SQL LOGGING  ##################

# def set_TIMINGSSIZE(n):
#     import pydal.adapters.base as _
#     _.TIMINGSSIZE = n

# from plugin_grand_helpers import set_TIMINGSSIZE
# set_TIMINGSSIZE(n)



# # results in web2py/web2py.log
# db._debug = True  # sql before exec to log

# # needs in web2py/logging.conf
# pyDAL handler
# [logger_pyDAL]
# level=DEBUG
# qualname=pyDAL
# handlers=consoleHandler,rotatingFileHandler
# propagate=0


############################### fake admin ###############################
if False:
    auth.has_permission= lambda *args, **kwargs: True
    from gluon.storage import Storage
    # auth.user= Storage( language_id = 1 )
    auth.user=db.auth_user(1) # Storage( language_id = 1 )
