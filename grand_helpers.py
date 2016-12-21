# -*- coding: utf-8 -*-
def get_fields_from_format(format_txt):  # TODO
    """
    >>> get_format_fields( '%(first_name)s %(last_name)s' )
    ['first_name',  'last_name']
    """
    import re
    regex = r"\%\((.+?)\)s"
    results = re.findall(regex, format_txt)
    return results
    
    

def grand_translation_val():  # TODO
    measurement_title_field = db.translation_field.with_alias('measurement_title').value.coalesce(
        db.measurement.title )
                
    db.translation_field.with_alias('measurement_title').on(
                    (db.translation_field.with_alias('measurement_title').tablename == 'measurement') &
                    (db.translation_field.with_alias('measurement_title').fieldname == 'title') &
                    (db.translation_field.with_alias('measurement_title').rid == db.measurement.id) &
                    (db.translation_field.with_alias('measurement_title').language_id == auth.user.language_id)
                )    
