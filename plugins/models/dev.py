# forces/tries modules reload on change

from gluon.custom_import import track_changes
track_changes(True)


from gluon import current

# Toggles limitby in DalView... for DBG purposes
current.dev_limitby = 1, 3


