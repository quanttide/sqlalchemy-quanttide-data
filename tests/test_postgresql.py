# -*- coding: utf-8 -*-

import unittest
import sys
import os

sys.path.insert(0, os.path.abspath('..'))

import sql_client.postgresql
import sql_client.sqlalchemy
import tests.base_case
import env


class SqlClientPostgresqlTestCase(tests.base_case.SqlClientTestCase):
    env = env.postgresql
    module = sql_client.postgresql

    def test_isolation_level(self):
        self._test_query([['READ COMMITTED']], 'show transaction isolation level')


class SqlClientSqlalchemyTestCase(tests.base_case.SqlClientTestCase):
    env = env.postgresql
    module = sql_client.sqlalchemy
    extra_kwargs = {'dialect': 'postgresql', 'origin_result': True}
    result_class = list

    def test_isolation_level(self):
        self._test_query([['READ COMMITTED']], 'show transaction isolation level')


class SqlClientPostgresqlPuncTestCase(SqlClientPostgresqlTestCase):
    env = env.postgresql_punc


class SqlClientSqlalchemyPuncTestCase(SqlClientSqlalchemyTestCase):
    env = env.postgresql_punc


class SqlClientPostgresqlPuncSpecTestCase(SqlClientPostgresqlTestCase):
    env = env.postgresql_punc_spec


class SqlClientSqlalchemyPuncSpecTestCase(SqlClientSqlalchemyTestCase):
    env = env.postgresql_punc_spec


if __name__ == '__main__':
    unittest.main()
