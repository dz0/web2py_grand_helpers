
from gluon import current

# messages defined in  models/a_config.py
MSGS = """MSG_NO_RECORDS_FOUND
MSG_SUCCESS
MSG_ERRORS_IN_FORM
MSG_ACTION_FAILED
MSG_NO_PERMISSION
MSG_RECORD_NOT_FOUND
MSG_INVALID_ACTION"""
for msg in MSGS.split():
    if getattr(current, 'DBG', None):
        val = globals().get(msg, msg)
    else:
        val = globals()[msg]
    setattr(current, msg,  val)

current.auth = auth
current.db = db


# monkeypach the sql log (dbstats or _timings) size
from plugin_grand_helpers import set_TIMINGSSIZE
# set_TIMINGSSIZE(100)

