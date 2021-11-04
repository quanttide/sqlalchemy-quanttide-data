# -*- coding: utf-8 -*-

import unittest


class SqlClientTestCase(unittest.TestCase):
    env = None
    module = None
    extra_kwargs = {}
    result_class = tuple

    @classmethod
    def setUpClass(cls) -> None:
        cls.table = cls.env['table']
        cls.db = cls.module.SqlClient(try_times_connect=1, raise_error=True, **cls.env, **cls.extra_kwargs)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.query('delete from {}'.format(cls.table))
        cls.db.close()

    def setUp(self) -> None:
        self.db.query('delete from {}'.format(self.table))

    def _query_middleware(self, query: str, args=None, query_func=None, **kwargs):
        if query_func is None:
            query_func = self.db.query
        return query_func(query, args, **kwargs)

    def _test_query(self, result, query, args=None, to_result_class=True, map_tuple=True, **kwargs):
        self.assertEqual(result if not to_result_class else self.result_class(map(
            tuple, result)) if map_tuple else self.result_class(result), self._query_middleware(query, args, **kwargs))

    def _subtest_query(self, result, query, args=None, msg=None, to_result_class=True, map_tuple=True, **kwargs):
        if to_result_class:
            result = self.result_class(map(tuple, result)) if map_tuple else self.result_class(result)
        with self.subTest(*() if msg is None else (msg,), result=result, query=query, args=args, kwargs=kwargs):
            self.assertEqual(result, self._query_middleware(query, args, **kwargs))

    def test_query(self):
        self._subtest_query([[1]], 'select 1 from dual')
        self._subtest_query([['2']], 'select %(a)s from dual', {'a': '2'})
        self._subtest_query([['2']], 'select :a from dual', {'a': '2'})
        self._subtest_query([[2]], 'select :1 from dual', {'1': 2})
        self._test_query(1, 'insert into {} values (1,2)'.format(self.table), to_result_class=False, fetchall=False)
        self._test_query([], 'insert into {} values (%s,%s)'.format(self.table), ('3', 4))
        self._test_query(1, 'insert into {} values (:a,:b)'.format(self.table), {'a': '23', 'b': '24'},
                         to_result_class=False, fetchall=False)
        self._subtest_query([['1', '2'], ['3', '4'], ['23', '24']], 'select * from {}'.format(self.table))
        self._subtest_query([{'a': '1', 'b': '2'}, {'a': '3', 'b': '4'}, {'a': '23', 'b': '24'}],
                            'select * from {}'.format(self.table), to_result_class=False, dictionary=True)
        self._subtest_query(1, 'select * from {}'.format(self.table), to_result_class=False, fetchall=False)
        self._subtest_query(1, 'select * from {}'.format(self.table), to_result_class=False, fetchall=False,
                            dictionary=True)

    def test_query_isolation(self):
        self.db.autocommit = False
        try:
            self.db.begin()
            self._test_query(1, 'insert into {} values (500,600)'.format(self.table), to_result_class=False,
                             fetchall=False)
            self._subtest_query([['500', '600']], 'select * from {}'.format(self.table), msg='first')
            self.db.try_connect()
            if hasattr(self.db, 'create_engine'):
                self._subtest_query([], 'select * from {}'.format(self.table),
                                    msg='after reconnect without recreate engine')
                self.db.create_engine()
                self.db.try_connect()
            self._subtest_query([], 'select * from {}'.format(self.table), msg='after reconnect')
            new_db = self.module.SqlClient(autocommit=False, try_times_connect=1, raise_error=True, **self.env,
                                           **self.extra_kwargs)
            new_db.begin()
            self._subtest_query([], 'select * from {}'.format(self.table), msg='new db', query_func=new_db.query)
            self._test_query(1, 'insert into {} values (700,800)'.format(self.table), to_result_class=False,
                             query_func=new_db.query, fetchall=False)
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
            self.db.autocommit = True
            self.db.rollback()

    def test_save_data(self):
        self.assertEqual(1, self.db.save_data((5, 6),
                                              self.table.replace('{', '{{').replace('}', '}}').replace('?', '\?')))
        self._test_query([['5', '6']], 'select * from {}'.format(self.table))
