from gluon.storage import Storage
from gluon import current

from pydal.objects import Field, Row, Expression
from pydal.objects import SQLALL, Query

from plugin_DalView import DalView
from plugin_grand_helpers import test_fields


# for Validator with translator
from gluon.validators import IS_IN_DB
from pydal.objects import Field, FieldVirtual, FieldMethod
from gluon.sqlhtml import AutocompleteWidget
from gluon.html import OPTION, SELECT





class GrandTranslator():
    def __init__(self, fields, language_id=None):
        db = self.db = current.db

        # self.db_adapter = self.db._adapter


        if not language_id:
            if auth.is_logged_in():
                language_id = auth.user.language_id
            else:
                try:
                    language_id = db(db.language.main == True).select().first().id
                except:
                    raise RuntimeError("No language defined for translating")

        self.language_id = language_id

        if fields:
            self.fields = self.db._adapter.expand_all(fields, [])
            #self.try_auto_update_fields = False

        else:
            # find fields from DB -- and possibly store in session
            session = current.session

            # make a singleton per session
            if not session.translatable_fields:
                rows = db().select( db.translation_field.tablename , db.translation_field.fieldname  , distinct=True )

                session.translatable_fields = []

                for r in rows:
                    try:
                        session.translatable_fields.append( db[ r.tablename ][ r.fieldname ] )
                    except Exception as e:
                        raise RuntimeWarning( ("Translation warning: %(tablename).%(fieldname) not found in db.\n"  % r ) + str(e) )

            self.fields = session.translatable_fields
            # self.try_auto_update_fields = True
            #self.get_all_translatable_fields()

    def translation_alias(self, field):
        """aliased translations table
        """
        return self.db.translation_field.with_alias( "T_"+field._tablename+"__"+field.name )

    def translate_field(self, field):
        if str(field) in map(str, self.fields):  # direct check probably uses __eq__ for objects and returns nonsense
            t_alias = self.translation_alias( field )
            if not str(field) in  map(str, self.affected_fields):
                self.affected_fields.append(field)
            return  t_alias.value.coalesce( field )
            # return  self.adapter.COALESCE( t_alias.value , field)
        else:
            return field

    def generate_left_joins(self):
        joins = []
        for field in self.affected_fields:
            t_alias = self.translation_alias(field)
            joins.append(
                t_alias.on(
                    (t_alias.tablename == field._tablename) &
                     (t_alias.fieldname == field.name) &  # for aliased fields might need different
                     (t_alias.rid == field._table._id) &
                     (t_alias.language_id == self.language_id)
                )
            )
        return joins

    def is_translation(self, expr):
        '''see is_expression_translated'''
        return (
              hasattr(expr, 'op') and expr.op is expr.db._adapter.COALESCE
              and isinstance(expr.second, Field)
              and str(expr.first) in [self.translation_alias(expr.second) + '.value', 'translation_field.value']
            )

    def is_expression_translated(self,expr):  # for more consice API
        return self.is_translation(expr)

    def is_validator_translated(self, validator):
        return isinstance(validator, T_IS_IN_DB)

    def is_widget_translated(self, widget):
        return isinstance(widget, T_AutocompleteWidget)

    def translate(self, expression ):
        """Traverse Expression (or Query) tree and   decorate  fields with COALESCE translations
        returns:
           new expression
           left_joins  for translations
        """

        self.affected_fields = [ ]
        # self.new_expression = Expression(db,lambda item:item)

        # maybe use ideas from https://gist.github.com/Xjs/114831
        def _traverse_translate( expr, inplace=False):
            # based on base adapter "expand"

            if expr is None:
                return None

            if isinstance(expr, Field):
                return  self.translate_field( expr )

            # prevent translations of in aggregates...
            #  self.db._adapter.COUNT is sensitive to translation
            # not sure about CONCAT   SUM of texts ?
            elif hasattr(expr, 'op') and expr.op is self.db._adapter.AGGREGATE :
                return expr

            #if we already have translation here
            elif self.is_translation(expr):
                return expr

            elif isinstance(expr, (Expression, Query)):
                first =  _traverse_translate(  expr.first )
                second =  _traverse_translate( expr.second )

                # if inplace:
                #     expr.first = first
                #     expr.second = second
                #     return

                return expr.__class__( expr.db, expr.op, first, second )
                # return Expression( expr.db, expr.op, first, second, expr.type )
                # return Query( expr.db, expr.op, first, second )

            elif isinstance(expr, SQLALL):
                  expr = expr._table.fields  # might be problems with flattening
                  return [_traverse_translate(e) for e in expr]

            elif isinstance(expr, (list, tuple )):
                flatten_ALL = []
                for e in expr:
                    if isinstance(expr, SQLALL): # expand and flatten
                        flatten_ALL.extend( expr._table.fields )
                    else:
                        flatten_ALL.append(e)

                return [_traverse_translate(e) for e in flatten_ALL]
            else:
                return expr
        
        new_expression = _traverse_translate( expression )
        self.result = Storage( expr=new_expression,
                               left=self.generate_left_joins(),
                               affected_fields=self.affected_fields
                               )

        return self.result



# Validator with translator
from gluon.http import HTTP

class T_AutocompleteWidget( AutocompleteWidget ):
    def __init__( self, translator, *args, **kwargs):
        self.translator = translator
        AutocompleteWidget.__init__ (self, *args, **kwargs)

    def callback(self):
        if self.keyword in self.request.vars:
            field = self.fields[0]

            rows = DalView(*(self.fields+self.help_fields),
                           translator=self.translator,

                           query=field.contains(self.request.vars[self.keyword], case_sensitive=False),
                           # query=field.like(self.request.vars[self.keyword] + '%', case_sensitive=False),
                           orderby=self.orderby, limitby=self.limitby, distinct=self.distinct
                           ).execute() # compact=False
            # rows.compact = True # peculiarities of DAL..

            # rows = self.db(field.like(self.request.vars[self.keyword] + '%', case_sensitive=False)).select(orderby=self.orderby, limitby=self.limitby, distinct=self.distinct, *(self.fields+self.help_fields))

            if rows:
                if self.is_reference:
                    id_field = self.fields[1]
                    if self.help_fields:
                        options = [OPTION(
                            self.help_string % dict([(h.name, s[h.name]) for h in self.fields[:1] + self.help_fields]),
                                   _value=s[id_field.name], _selected=(k == 0)) for k, s in enumerate(rows)]
                    else:
                        options = [OPTION(
                            s[field.name], _value=s[id_field.name],
                            _selected=(k == 0)) for k, s in enumerate(rows)]
                    raise HTTP(
                        200, SELECT(_id=self.keyword, _class='autocomplete',
                                    _size=len(rows), _multiple=(len(rows) == 1),
                                    *options).xml())
                else:
                    raise HTTP(
                        200, SELECT(_id=self.keyword, _class='autocomplete',
                                    _size=len(rows), _multiple=(len(rows) == 1),
                                    *[OPTION(s[field.name],
                                             _selected=(k == 0))
                                      for k, s in enumerate(rows)]).xml())
            else:
                raise HTTP(200, '')

    def callback_NEWw2p(self):
        if self.keyword in self.request.vars:
            field = self.fields[0]
            if type(field) is Field.Virtual:
                records = []
                table_rows = self.db(self.db[field.tablename]).select(orderby=self.orderby)
                count = 0
                for row in table_rows:
                    if self.at_beginning:
                        if row[field.name].lower().startswith(self.request.vars[self.keyword]):
                            count += 1
                            records.append(row)
                    else:
                        if self.request.vars[self.keyword] in row[field.name].lower():
                            count += 1
                            records.append(row)
                    if count == 10:
                        break
                rows = Rows(self.db, records, table_rows.colnames, compact=table_rows.compact)
            else:

            # elif settings and settings.global_settings.web2py_runtime_gae:
            #     rows = self.db(field.__ge__(self.request.vars[self.keyword]) & field.__lt__(self.request.vars[self.keyword] + u'\ufffd')).select(orderby=self.orderby, limitby=self.limitby, *(self.fields+self.help_fields))
            # elif self.at_beginning:
            #     rows = self.db(field.like(self.request.vars[self.keyword] + '%', case_sensitive=False)).select(orderby=self.orderby, limitby=self.limitby, distinct=self.distinct, *(self.fields+self.help_fields))
            # else:
            #     rows = self.db(field.contains(self.request.vars[self.keyword], case_sensitive=False)).select(orderby=self.orderby, limitby=self.limitby, distinct=self.distinct, *(self.fields+self.help_fields))

                rows = DalView(*(self.fields + self.help_fields),
                               translator=self.translator,

                               query=field.like(self.request.vars[self.keyword] + '%', case_sensitive=False),
                               orderby=self.orderby, limitby=self.limitby, distinct=self.distinct
                               ).execute() # compact=False
            if rows:
                if self.is_reference:
                    id_field = self.fields[1]
                    if self.help_fields:
                        options = [OPTION(
                            self.help_string % dict([(h.name, s[h.name]) for h in self.fields[:1] + self.help_fields]),
                                   _value=s[id_field.name], _selected=(k == 0)) for k, s in enumerate(rows)]
                    else:
                        options = [OPTION(
                            s[field.name], _value=s[id_field.name],
                            _selected=(k == 0)) for k, s in enumerate(rows)]
                    raise HTTP(
                        200, SELECT(_id=self.keyword, _class='autocomplete',
                                    _size=len(rows), _multiple=(len(rows) == 1),
                                    *options).xml())
                else:
                    raise HTTP(
                        200, SELECT(_id=self.keyword, _class='autocomplete',
                                    _size=len(rows), _multiple=(len(rows) == 1),
                                    *[OPTION(s[field.name],
                                             _selected=(k == 0))
                                      for k, s in enumerate(rows)]).xml())
            else:
                raise HTTP(200, '')


class T_IS_IN_DB(IS_IN_DB):
    def __init__( self, translator, dbset, field, *args, **kwargs):
        self.translator = translator
        IS_IN_DB.__init__ (self, dbset, field, *args, **kwargs)

    #override
    def build_set(self):
        table = self.dbset.db[self.ktable]

        # workaround for backwards compatibility
        if hasattr(self, 'fields'):
            self.fieldnames = getattr(self, 'fieldnames', self.fields)
        else:
            self.fields = getattr(self, 'fields', self.fieldnames)


        if self.fields == 'all' or self.fieldnames == '*':
            fields = [f for f in table]
        else:
            fields = [table[k] for k in self.fieldnames]
        ignore = (FieldVirtual, FieldMethod)
        fields = filter(lambda f: not isinstance(f, ignore), fields)
        if self.dbset.db._dbname != 'gae':
            orderby = self.orderby or reduce(lambda a, b: a | b, fields)
            groupby = self.groupby
            distinct = self.distinct
            left = self.left
            dd = dict(orderby=orderby, groupby=groupby,
                      distinct=distinct, cache=self.cache,
                      cacheable=True, left=left)
            # records = self.dbset(table).select(*fields, **dd)
            records = DalView( *fields, translator=self.translator, query=self.dbset(table).query, **dd).execute() # compact=False

        # records.compact = True # todo: somehow make it more fluent to work - execute should probably get the same compact=... ?
        # self.theset = [str(r[self.kfield]) for r in records]
        self.theset = [str(r[self.kfield]) for r in records]
        if isinstance(self.label, str):
            self.labels = [self.label % r for r in records]
        else:
            self.labels = [self.label(r) for r in records]
