from gluon import current
current.db = db

SEARCHING_GRID = 'searching/searching_grid.html'

# IS_MOBILE = config_extra.get('is_mobile')
IS_MOBILE = 0
IS_MOBILE = bool(int(IS_MOBILE)) if IS_MOBILE else request.user_agent().is_mobile

def DBG():
    return request.controller == 'maintenance' 


# from helpers import TOTAL_ROWS
TOTAL_ROWS = "42" # :)
######################################
#
# for testing (based on grand3 stuff)

# from helpers import represent_boolean, represent_datetime, validate_client_login_form
# #from lib.core import default_language, delete_user_sessions
# from lib.validators import IS_TIME_IN_FORMAT
# from lib.translation import set_language

# import datetime

# USER_TYPES = (
    # ('administration', T('user__type_administration')),
    # ('production', T('user__type_production')),
    # ('service', T('user__type_service')),
    # ('client', T('user__type_client'))
# )

# USER_DATA_LEVELS = (
    # ('user', T('user__data_level_user')),
    # ('branch', T('user__data_level_branch')),
    # ('all', T('user__data_level_all'))
# )

# auth.settings.extra_fields['auth_user'] = [
    # Field('type', requires=IS_EMPTY_OR(IS_IN_SET(USER_TYPES)), label=T('user__type'),
          # represent=lambda value: T('user__type_{0}'.format(value)) if value else ''),
    # #Field('language_id', 'reference language', requires=IS_IN_DB(db, 'language.id', db.language._format, zero=None),
          # #label=T('user__language'), default=default_language(db), ondelete='RESTRICT',
          # #represent=lambda value: db.language._format % db.language[value]),
    # Field('active', 'boolean', label=T('user__active'), default=False, represent=represent_boolean),
    # Field('color', label=T('user__color')),
    # Field('data_level', requires=IS_IN_SET(USER_DATA_LEVELS), label=T('user__data_level'), default='user',
          # represent=lambda value: T('user__data_level_{0}'.format(value)) if value else ''),
    # Field('calendar_time_from', 'time', label=T('user__calendar_time_from'), default=datetime.time(7),
          # requires=IS_TIME_IN_FORMAT()),
    # Field('calendar_time_until', 'time', label=T('user__calendar_time_until'), default=datetime.time(18),
          # requires=IS_TIME_IN_FORMAT()),
    # Field('email_managed_tasks_only', 'boolean', label=T('user__email_managed_tasks_only'), default=False),
    # Field('registry_table_height', 'integer', label=T('user__registry_table_height'), default=400,
          # requires=IS_INT_IN_RANGE())
# ]
