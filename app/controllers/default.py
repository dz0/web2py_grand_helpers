# -*- coding: utf-8 -*-

def index():
    """
    return auth.wiki() ?
    """
    response.flash = T("Hello World")
    return dict(
                menu = MENU( [
                  ('plugin_joins_builder', False, URL(c='plugin_joins_builder', f='index')),
                  ('plugin_joins_builder', False, URL(c='plugin_search_form', f='index')),
                ] )
            )



