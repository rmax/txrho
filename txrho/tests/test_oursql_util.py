from unittest import TestCase

from txrho.oursql.util import (
    named_params,
)

class UtilsSqlTest(TestCase):

    def test_named_params(self):
        sql = 'foo ? ?'
        params = [1, 2]

        self.assertEqual(named_params(sql, params), (sql, params))

        sql = ':foo :bar'
        params = {'foo': 1, 'bar': 2}

        self.assertEqual(named_params(sql, params),
                         ('? ?', (1, 2)))

