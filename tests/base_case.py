# -*- coding: utf-8 -*-

import unittest
import sys
import os
from typing import Any

sys.path.insert(0, os.path.abspath('..'))

import sql_client.base


class SqlClientTestCase(unittest.TestCase):
    account = None
    module = None
    extra_kwargs = {}
    result_class = tuple
    db: sql_client.base.SqlClient = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.table = cls.account['table']
        cls.db = cls.module.SqlClient(try_times_connect=1, raise_error=True, **cls.account, **cls.extra_kwargs)
        cls.db.query('drop table if exists {}'.format(cls.table), fetchall=False)
        cls.db.query('create table {0} (a varchar(255) NULL,b varchar(255) NULL);'.format(cls.table), fetchall=False)

    def tearDown(self) -> None:
        self.db.query('delete from {}'.format(self.table), fetchall=False)

    def _query_middleware(self, query: str, args=None, query_func=None, **kwargs):
        if query_func is None:
            query_func = self.db.query
        return query_func(query, args, **kwargs)

    def _test_query(self, expected: Any, query, args=None, to_result_class=True, map_tuple=True, result_factory=None,
                    **kwargs):
        self.assertEqual(
            expected if not (to_result_class and isinstance(expected, (list, tuple))) else
            self.result_class(map(tuple, expected)) if map_tuple and expected and not isinstance(expected[0], dict) else
            self.result_class(expected), self._query_middleware(query, args, **kwargs) if result_factory is None else
            result_factory(self._query_middleware(query, args, **kwargs)))

    def _subtest_query(self, expected, query, args=None, msg=None, to_result_class=True, map_tuple=True,
                       result_factory=None, **kwargs):
        if to_result_class and isinstance(expected, (list, tuple)):
            expected = self.result_class(map(tuple, expected)) if map_tuple and expected and not isinstance(
                expected[0], dict) else self.result_class(expected)
        with self.subTest(*() if msg is None else (msg,), result=expected, query=query, args=args, kwargs=kwargs):
            self.assertEqual(expected,
                             self._query_middleware(query, args, **kwargs) if result_factory is None else
                             result_factory(self._query_middleware(query, args, **kwargs)))

    def test_query(self):
        self._subtest_query([[1]], 'select 1 from dual')
        self._subtest_query([['2']], 'select %(a)s from dual', {'a': '2'})
        self._subtest_query([['2']], 'select :a from dual', {'a': '2'})
        self._subtest_query([[2]], 'select :1 from dual', {'1': 2})
        self._test_query(1, 'insert into {} values (1,2)'.format(self.table), fetchall=False)
        self._test_query([], 'insert into {} values (%s,%s)'.format(self.table), ('3', 4))
        self._test_query(1, 'insert into {} values (:a,:b)'.format(self.table), {'a': '23', 'b': '24'}, fetchall=False)
        self._subtest_query([['1', '2'], ['3', '4'], ['23', '24']], 'select * from {}'.format(self.table))
        self._subtest_query([{'a': '1', 'b': '2'}, {'a': '3', 'b': '4'}, {'a': '23', 'b': '24'}],
                            'select * from {}'.format(self.table), dictionary=True)
        self._subtest_query(1, 'select * from {}'.format(self.table), fetchall=False)
        self._subtest_query(1, 'select * from {}'.format(self.table), fetchall=False, dictionary=True)

    def test_save_data(self):
        self.assertEqual(1, self.db.save_data((5, 6),
                                              self.table.replace('{', '{{').replace('}', '}}').replace('?', '\?')))
        self._test_query([['5', '6']], 'select * from {}'.format(self.table))
        self.assertEqual(2, self.db.save_data([{'a': 11, 'b': 12}, {'a': 9, 'b': 10}],
                                              self.table.replace('{', '{{').replace('}', '}}').replace('?', '\?')))
        self._test_query([['5', '6'], ['11', '12'], ['9', '10']], 'select * from {}'.format(self.table))
        self.assertEqual(1, self.db.save_data({'a': 7, 'b': 8},
                                              self.table.replace('{', '{{').replace('}', '}}').replace('?', '\?')))
        self._test_query([['5', '6'], ['11', '12'], ['9', '10'], ['7', '8']], 'select * from {}'.format(self.table))

    def test_autocommit(self):
        new_db = self.module.SqlClient(try_times_connect=1, raise_error=True, **self.account, **self.extra_kwargs)
        self.db.save_data((13, 14), self.table.replace('{', '{{').replace('}', '}}').replace('?', '\?'))
        self._test_query([['13', '14']], 'select * from {}'.format(self.table), query_func=new_db.query)
        self.db.autocommit = False
        try:
            self.db.begin()
            self.db.save_data((15, 16), self.table.replace('{', '{{').replace('}', '}}').replace('?', '\?'))
            self._test_query([['13', '14']], 'select * from {}'.format(self.table), query_func=new_db.query)
            new_db.close()
        finally:
            self.db.rollback()
            self.db.autocommit = True

    def test_query_isolation(self):
        self.db.autocommit = False
        try:
            self.db.begin()
            self._test_query(1, 'insert into {} values (500,600)'.format(self.table), fetchall=False)
            self._subtest_query([['500', '600']], 'select * from {}'.format(self.table), msg='first')
            self.db.try_connect()
            if hasattr(self.db, 'create_engine'):
                self._subtest_query([], 'select * from {}'.format(self.table),
                                    msg='after reconnect without recreate engine')
                self.db.create_engine()
                self.db.try_connect()
            self._subtest_query([], 'select * from {}'.format(self.table), msg='after reconnect')
            new_db = self.module.SqlClient(autocommit=False, try_times_connect=1, raise_error=True, **self.account,
                                           **self.extra_kwargs)
            new_db.begin()
            self._subtest_query([], 'select * from {}'.format(self.table), msg='new db', query_func=new_db.query)
            self._test_query(1, 'insert into {} values (700,800)'.format(self.table), query_func=new_db.query,
                             fetchall=False)
            self._subtest_query([], 'select * from {}'.format(self.table),
                                msg='old db after new db insert')
            self._subtest_query([['700', '800']], 'select * from {}'.format(self.table), msg='new db after insert',
                                query_func=new_db.query)
            new_db.try_connect()
            self._subtest_query([], 'select * from {}'.format(self.table), msg='new db after reconnect',
                                query_func=new_db.query)
            new_db.rollback()
            new_db.close()
        finally:
            self.db.rollback()
            self.db.autocommit = True

    def test_auto_reconnect(self):
        self.db.close()
        self._test_query(1, 'insert into {} values (1,2)'.format(self.table), fetchall=False)
        self._subtest_query([['1', '2']], 'select * from {}'.format(self.table))
