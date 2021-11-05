# -*- coding: utf-8 -*-

import unittest
import sys

sys.path.append('..')

import sql_client.sqlserver
import sql_client.sqlalchemy
import base_case
import env


class SqlClientSqlserverTestCase(base_case.SqlClientTestCase):
    env = env.sqlserver
    module = sql_client.sqlserver

    def test_isolation_level(self):
        self._test_query([['READ COMMITTED']], 'show transaction isolation level')


class SqlClientSqlalchemyTestCase(base_case.SqlClientTestCase):
    env = env.sqlserver
    module = sql_client.sqlalchemy
    extra_kwargs = {'dialect': 'mssql', 'origin_result': True}
    result_class = list

    def test_isolation_level(self):
        self._test_query([['READ COMMITTED']], 'show transaction isolation level')


class SqlClientSqlserverPuncTestCase(SqlClientSqlserverTestCase):
    env = env.sqlserver_punc


class SqlClientSqlalchemyPuncTestCase(SqlClientSqlalchemyTestCase):
    env = env.sqlserver_punc


class SqlClientSqlserverPuncSpecTestCase(SqlClientSqlserverTestCase):
    env = env.sqlserver_punc_spec


class SqlClientSqlalchemyPuncSpecTestCase(SqlClientSqlalchemyTestCase):
    env = env.sqlserver_punc_spec


if __name__ == '__main__':
    unittest.main()
