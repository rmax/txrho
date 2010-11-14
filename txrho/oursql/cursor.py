import itertools
import oursql

#
# All the code below is just to be able to use Row class in the cursor
#
class _RowDictWhateverMixin(object):
    def fetchone(self):
        row = super(_RowDictWhateverMixin, self).fetchone()
        if row is None:
            return None
        ret = Row()
        for name, value in itertools.izip(self.column_names, row):
            if name not in ret:
                ret[name] = value
            elif ret[name] != value:
                raise oursql.ProgrammingError('column "%s" appears more than '
                                              'once in output' % name)
        return ret


_RowDictResultSet = type('_RowDictResultSet',
                      (_RowDictWhateverMixin, oursql._ResultSet),
                      {'__module__': '__name__'})


_RowDictStatement = type('_RowDictStatement',
                      (_RowDictWhateverMixin, oursql._Statement),
                      {'__module__': '__name__'})


class _RowDictQuery(oursql._DictQuery):
    _result_class = _RowDictResultSet


class Row(dict):
    """A dict that allows attribute access"""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class RowDictCursor(oursql.Cursor):
    # our custom classes to use Row class
    _statement_class = _RowDictStatement
    _query_class = _RowDictQuery

