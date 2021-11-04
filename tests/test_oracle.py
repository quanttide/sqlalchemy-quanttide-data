# -*- coding: utf-8 -*-

import unittest
import sys

sys.path.append('..')

import sql_client.oracle
import sql_client.sqlalchemy
import base_case
import env


class SqlClientOracleTestCase(base_case.SqlClientTestCase):
    env = env.oracle
    module = sql_client.oracle


class SqlClientSqlalchemyTestCase(base_case.SqlClientTestCase):
    env = env.oracle
    module = sql_client.sqlalchemy
    extra_kwargs = {'dialect': 'oracle', 'origin_result': True}
    result_class = list

    def test_isolation_level(self):
        self.assertEqual('READ COMMITTED', self.db.connection.isolation_level)


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
