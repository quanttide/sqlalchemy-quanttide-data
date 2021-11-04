# -*- coding: utf-8 -*-

import unittest
import sys

sys.path.append('..')

import sql_client.pymysql
import sql_client.sqlalchemy
import base_case
import env


class SqlClientPymysqlTestCase(base_case.SqlClientTestCase):
    env = env.mysql
    module = sql_client.pymysql

    def test_isolation_level(self):
        version = self._query_middleware('SELECT VERSION()')[0][0].split('.')
        version[2] = version[2].split('-')[0]
        self._test_query([['REPEATABLE-READ']], 'SELECT @@transaction_isolation' if tuple(map(int, version)) >= (
            5, 7, 20) else 'SELECT @@tx_isolation')
        # 若版本不对: pymysql.err.InternalError: (1193, "Unknown system variable 'transaction_isolation'")

    def query_middleware(self, query: str, args=None, query_func=None, **kwargs):
        if query_func is None:
            query_func = self.db.query
        if args is not None and '%' in self.table:
            query = query.replace(self.table, self.table.replace('%', '%%'))
        return query_func(query, args, **kwargs)

    def test_save_data(self):
        self.assertEqual(1, self.db.save_data((5, 6), self.table.replace('{', '{{').replace('}', '}}').replace(
            '?', '\?').replace('%', '%%')))
        self._test_query([['5', '6']], 'select * from {}'.format(self.table))


class SqlClientSqlalchemyTestCase(base_case.SqlClientTestCase):
    env = env.mysql
    module = sql_client.sqlalchemy
    extra_kwargs = {'dialect': 'mysql+pymysql', 'origin_result': True}
    result_class = list

    def test_isolation_level(self):
        version = self._query_middleware('SELECT VERSION()')[0][0].split('.')
        version[2] = version[2].split('-')[0]
        self._test_query([['REPEATABLE-READ']], 'SELECT @@transaction_isolation' if tuple(map(int, version)) >= (
            5, 7, 20) else 'SELECT @@tx_isolation')
        # 若版本不对: sqlalchemy.exc.InternalError: (pymysql.err.InternalError) (1193, "Unknown system variable 'transaction_isolation'")


class SqlClientPymysqlPuncTestCase(SqlClientPymysqlTestCase):
    env = env.mysql_punc


class SqlClientSqlalchemyPuncTestCase(SqlClientSqlalchemyTestCase):
    env = env.mysql_punc


class SqlClientPymysqlPuncSpecTestCase(SqlClientPymysqlTestCase):
    env = env.mysql_punc_spec


class SqlClientSqlalchemyPuncSpecTestCase(SqlClientSqlalchemyTestCase):
    env = env.mysql_punc_spec


if __name__ == '__main__':
    unittest.main()
