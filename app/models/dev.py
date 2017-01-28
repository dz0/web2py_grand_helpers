# forces/tries modules reload on change


db.define_table('language',
    Field('article', 'string'),
    Field('main', 'boolean'),
    Field('order_no', 'integer', label='#'
            # , 
            # default=lambda: create_order_no(db, 'language'),
            # represent=lambda value: represent_order_no(db, 'language', value, 'sticker_languages')
          ),
    format='%(article)s'
)


# plugin_translations
"""
db.define_table('translation_key',
    Field('context_id', 'reference context'),
    Field('key', 'string')
)

db.define_table('translation_translation',
    Field('key_id', 'reference translation_key'),
    Field('language_id', 'reference language'),
    Field('value', 'string')
)
"""

db.define_table('translation_field',
    Field('tablename'),
    Field('fieldname'),
    Field('rid', 'integer'),
    Field('language_id', 'reference language'),
    Field('value', 'text')
)

from gluon.custom_import import track_changes
track_changes(True)



