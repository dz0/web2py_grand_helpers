from gluon import current
current.db = db
current.auth = auth


# IS_MOBILE = config_extra.get('is_mobile')
IS_MOBILE = 0
IS_MOBILE = bool(int(IS_MOBILE)) if IS_MOBILE else request.user_agent().is_mobile

def DBG():
    return request.controller.startswith('plugin')  # 'plugin_w2ui_grid' # 'maintenance' 

if DBG():
    SEARCHING_GRID = 'searching/searching_grid.html'
    auth.has_permission = lambda (permision_name,  data_name): True


try:
    MSG_NO_PERMISSION
    MSG_ACTION_FAILED
except NameError as e:
    MSG_NO_PERMISSION = "MSG_NO_PERMISSION"
    MSG_ACTION_FAILED = "MSG_ACTION_FAILED"

current.DBG = DBG  # TODO -- move to requests.DBG or request.vars.DBG
current.MSG_NO_PERMISSION = MSG_NO_PERMISSION
current.MSG_ACTION_FAILED = MSG_ACTION_FAILED
   
