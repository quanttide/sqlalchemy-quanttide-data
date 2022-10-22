# -*- coding: utf-8 -*-

import unittest
import sys
import os

sys.path.insert(0, os.path.abspath('..'))

import sql_client.oracle
import sql_client.sqlalchemy
import tests.base_case
import env


class SqlClientOracleTestCase(tests.base_case.SqlClientTestCase):
    env = env.oracle
    module = sql_client.oracle

    def test_query(self):
        self._subtest_query([[1]], 'select 1 from dual')
        self._subtest_query([['2']], 'select %(a)s from dual', {'a': '2'})
        self._subtest_query([['2']], 'select :a from dual', {'a': '2'})
        self._subtest_query([['2']], 'select :1 from dual', {'1': 2})
        self._test_query(1, 'insert into {} values (1,2)'.format(self.table), to_result_class=False, fetchall=False)
        self._test_query([], 'insert into {} values (%s,%s)'.format(self.table), ('3', 4))
        self._test_query(1, 'insert into {} values (:a,:b)'.format(self.table), {'a': '23', 'b': '24'},
                         to_result_class=False, fetchall=False)
        self._subtest_query([['1', '2'], ['23', '24'], ['3', '4']], 'select * from {}'.format(self.table),
                            result_factory=lambda x: sorted(x))
        self._subtest_query([{'A': '1', 'B': '2'}, {'A': '23', 'B': '24'}, {'A': '3', 'B': '4'}],
                            'select * from {}'.format(self.table), to_result_class=False,
                            result_factory=lambda x: sorted(x, key=lambda x: next(x.values())), dictionary=True)
        self._subtest_query(1, 'select * from {}'.format(self.table), to_result_class=False, fetchall=False)
        self._subtest_query(1, 'select * from {}'.format(self.table), to_result_class=False, fetchall=False,
                            dictionary=True)


class SqlClientSqlalchemyTestCase(tests.base_case.SqlClientTestCase):
    env = env.oracle
    module = sql_client.sqlalchemy
    extra_kwargs = {'dialect': 'oracle', 'origin_result': True}
    result_class = list

    def test_isolation_level(self):
        self.assertEqual('READ COMMITTED', self.db.connection.isolation_level)

    def test_query(self):
        self._subtest_query([[1]], 'select 1 from dual')
        self._subtest_query([['2']], 'select %(a)s from dual', {'a': '2'})
        self._subtest_query([['2']], 'select :a from dual', {'a': '2'})
        self._subtest_query([[2]], 'select :1 from dual', {'1': 2})
        self._test_query(1, 'insert into {} values (1,2)'.format(self.table), to_result_class=False, fetchall=False)
        self._test_query([], 'insert into {} values (%s,%s)'.format(self.table), ('3', 4))
        self._test_query(1, 'insert into {} values (:a,:b)'.format(self.table), {'a': '23', 'b': '24'},
                         to_result_class=False, fetchall=False)
        self._subtest_query([['1', '2'], ['23', '24'], ['3', '4']], 'select * from {}'.format(self.table),
                            result_factory=lambda x: sorted(x))
        self._subtest_query([{'a': '1', 'b': '2'}, {'a': '23', 'b': '24'}, {'a': '3', 'b': '4'}],
                            'select * from {}'.format(self.table), to_result_class=False,
                            result_factory=lambda x: sorted(x, key=lambda x: next(x.values())), dictionary=True)
        self._subtest_query(1, 'select * from {}'.format(self.table), to_result_class=False, fetchall=False)
        self._subtest_query(1, 'select * from {}'.format(self.table), to_result_class=False, fetchall=False,
                            dictionary=True)


class SqlClientOraclePuncTestCase(SqlClientOracleTestCase):
    env = env.oracle_punc


class SqlClientSqlalchemyPuncTestCase(SqlClientSqlalchemyTestCase):
    env = env.oracle_punc


class SqlClientOraclePuncSpecTestCase(SqlClientOracleTestCase):
    env = env.oracle_punc_spec


class SqlClientSqlalchemyPuncSpecTestCase(SqlClientSqlalchemyTestCase):
    env = env.oracle_punc_spec


if __name__ == '__main__':
    unittest.main()
