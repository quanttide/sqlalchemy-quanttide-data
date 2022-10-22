# -*- coding: utf-8 -*-

import unittest
import sys
import os

sys.path.insert(0, os.path.abspath('..'))

import sql_client.sqlserver
import sql_client.sqlalchemy
import tests.base_case
import env


class SqlClientSqlserverTestCase(tests.base_case.SqlClientTestCase):
    env = env.sqlserver
    module = sql_client.sqlserver

    def test_isolation_level(self):
        self._test_query([['READ COMMITTED']], 'show transaction isolation level')


class SqlClientSqlalchemyTestCase(tests.base_case.SqlClientTestCase):
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
