# -*- coding: utf-8 -*-

def index():
    """
    return auth.wiki() ?
    """
    response.flash = T("Hello World")
    return dict(
                menu = MENU( [
                  ('plugin_grand_helpers_TESTALL', False, URL(c='plugin_grand_helpers_TESTALL', f='index')),
                  ('plugin_AnySQLFORM', False, URL(c='plugin_AnySQLFORM', f='index')),
                  ('plugin_joins_builder', False, URL(c='plugin_joins_builder', f='index')),
                ] )
            )



