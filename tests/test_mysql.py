# -*- coding: utf-8 -*-

import unittest
import sys
import os

sys.path.insert(0, os.path.abspath('..'))

import sql_client.pymysql
import sql_client.mysqlclient
import sql_client.sqlalchemy
import tests.base_case
import env


class SqlClientMysqlclientTestCase(tests.base_case.SqlClientTestCase):
    account = env.mysql
    module = sql_client.mysqlclient

    def _query_middleware(self, query: str, args=None, query_func=None, **kwargs):
        if query_func is None:
            query_func = self.db.query
        if args is not None and '%' in self.table:
            query = query.replace(self.table, self.table.replace('%', '%%'))
        return query_func(query, args, **kwargs)

    def test_save_data(self):
        self.assertEqual(1, self.db.save_data((5, 6), self.table.replace('{', '{{').replace('}', '}}').replace(
            '?', '\?').replace('%', '%%')))
        self._test_query([['5', '6']], 'select * from {}'.format(self.table))
        self.assertEqual(2, self.db.save_data([{'a': 11, 'b': 12}, {'a': 9, 'b': 10}], self.table.replace(
            '{', '{{').replace('}', '}}').replace('?', '\?').replace('%', '%%')))
        self._test_query([['5', '6'], ['11', '12'], ['9', '10']], 'select * from {}'.format(self.table))
        self.assertEqual(1, self.db.save_data({'a': 7, 'b': 8}, self.table.replace('{', '{{').replace(
            '}', '}}').replace('?', '\?').replace('%', '%%')))
        self._test_query([['5', '6'], ['11', '12'], ['9', '10'], ['7', '8']], 'select * from {}'.format(self.table))

    def test_isolation_level(self):
        version = self._query_middleware('SELECT VERSION()')[0][0].split('.')
        version[2] = version[2].split('-')[0]
        self._test_query([['REPEATABLE-READ']], 'SELECT @@transaction_isolation' if tuple(map(int, version)) >= (
            5, 7, 20) else 'SELECT @@tx_isolation')
        # 若版本不对: pymysql.err.InternalError: (1193, "Unknown system variable 'transaction_isolation'")


class SqlClientPymysqlTestCase(SqlClientMysqlclientTestCase):
    module = sql_client.pymysql

    def test_query(self):
        # pymysql dictionary=True时返回list而非tuple
        self._subtest_query([[1]], 'select 1 from dual')
        self._subtest_query([['2']], 'select %(a)s from dual', {'a': '2'})
        self._subtest_query([['2']], 'select :a from dual', {'a': '2'})
        self._subtest_query([[2]], 'select :1 from dual', {'1': 2})
        self._test_query(1, 'insert into {} values (1,2)'.format(self.table), fetchall=False)
        self._test_query([], 'insert into {} values (%s,%s)'.format(self.table), ('3', 4))
        self._test_query(1, 'insert into {} values (:a,:b)'.format(self.table), {'a': '23', 'b': '24'}, fetchall=False)
        self._subtest_query([['1', '2'], ['3', '4'], ['23', '24']], 'select * from {}'.format(self.table))
        self._subtest_query([{'a': '1', 'b': '2'}, {'a': '3', 'b': '4'}, {'a': '23', 'b': '24'}],
                            'select * from {}'.format(self.table), to_result_class=False, dictionary=True)
        self._subtest_query(1, 'select * from {}'.format(self.table), fetchall=False)
        self._subtest_query(1, 'select * from {}'.format(self.table), fetchall=False, dictionary=True)


class SqlClientSqlalchemyTestCase(tests.base_case.SqlClientTestCase):
    account = env.mysql
    module = sql_client.sqlalchemy
    extra_kwargs = {'dialect': 'mysql+pymysql', 'origin_result': True}
    result_class = list

    def test_isolation_level(self):
        version = self._query_middleware('SELECT VERSION()')[0][0].split('.')
        version[2] = version[2].split('-')[0]
        self._test_query([['REPEATABLE-READ']], 'SELECT @@transaction_isolation' if tuple(map(int, version)) >= (
            5, 7, 20) else 'SELECT @@tx_isolation')
        # 若版本不对: sqlalchemy.exc.InternalError: (pymysql.err.InternalError) (1193, "Unknown system variable 'transaction_isolation'")


class SqlClientMysqlclientPuncTestCase(SqlClientMysqlclientTestCase):
    account = env.mysql_punc


class SqlClientPymysqlPuncTestCase(SqlClientPymysqlTestCase):
    account = env.mysql_punc


class SqlClientSqlalchemyPuncTestCase(SqlClientSqlalchemyTestCase):
    account = env.mysql_punc


class SqlClientMysqlclientPuncSpecTestCase(SqlClientMysqlclientTestCase):
    account = env.mysql_punc_spec


class SqlClientPymysqlPuncSpecTestCase(SqlClientPymysqlTestCase):
    account = env.mysql_punc_spec


class SqlClientSqlalchemyPuncSpecTestCase(SqlClientSqlalchemyTestCase):
    account = env.mysql_punc_spec


if __name__ == '__main__':
    unittest.main()
