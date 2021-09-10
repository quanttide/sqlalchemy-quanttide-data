# -*- coding: utf-8 -*-

import sys

sys.path.append('..')

import psycopg2

import sql_client.postgresql
import sql_client.sqlalchemy
import env

print('psycopg2:')
connection = psycopg2.connect(**env.postgresql)
connection.autocommit = True
cursor = connection.cursor()
cursor.execute('delete from t')
cursor.execute('insert into t values (1,2)', None)
cursor.executemany('insert into t values (010,020)', [1, 2])
cursor.execute('select %(a)s from dual', {'a': '2'})
# cursor.execute('select %(a)s from dual', a='3')
# data = cursor.fetchone()
# data = cursor.fetchmany(8)
cursor.execute('select * from t')
data = cursor.fetchall()
print(data)
cursor.close()
connection.commit()
connection.close()

print('\nsql_client.postgresql:')
db = sql_client.postgresql.SqlClient(raise_error=True, **env.postgresql)
# print(db.query('insert into t values (1,2)', fetchall=False))
print(db.query('insert into t values (%s,%s)', ('3', 4), fetchall=False))
print(db.save_data((5, 6), 't'))
print(db.query('select %(a)s from dual', {'a': '2'}))
print(db.query('select 1 from dual'))
print(db.query('select * from t'))

print('\nsql_client.sqlalchemy:')
db = sql_client.sqlalchemy.SqlClient(dialect='postgresql', raise_error=True, **env.postgresql)
# print(db.query('insert into t values (1,2)', fetchall=False))
print(db.query('insert into t values (:a,:b)', {'a': '23', 'b': '24'}, fetchall=False))
print(db.query('insert into t values (%(a)s,%(b)s)', {'a': '23', 'b': '24'}, fetchall=False, raise_error=True))
print(db.save_data(('21', '22'), 't'))
print(db.query('select :a from dual', {'a': '2'}, origin_result=True))
print(db.query('select :1 from dual', {'1': 2}, origin_result=True))
print(db.query('select 1 from dual', origin_result=True))
print(db.query('select * from t', origin_result=True))
print(db.__dict__)
print(db.connection.get_isolation_level())
db.autocommit = False
print(db.query('insert into t values (500,600)', fetchall=False))
print(db.query('select * from t', origin_result=True))
db.try_connect()
print(db.query('select * from t', origin_result=True))
db = sql_client.sqlalchemy.SqlClient(dialect='postgresql', autocommit=False, raise_error=True, **env.postgresql)
print(db.query('select * from t', origin_result=True))
print(db.query('insert into t values (700,800)', fetchall=False))
print(db.query('select * from t', origin_result=True))
db.try_connect()
print(db.query('select * from t', origin_result=True))

# ------punctuation tests------
table = '"' + ' !"#$%&%40\'()*+,-./:;<=>?@[]\\\_\\0^`{|} ~t '.replace('"', '""') + '"'

print('\npsycopg2(punc):')
connection = psycopg2.connect(**env.postgresql_punc)
connection.autocommit = True
cursor = connection.cursor()
cursor.execute('delete from {}'.format(table))
cursor.execute('insert into {} values (1,2)'.format(table), None)
cursor.executemany('insert into {} values (010,020)'.format(table.replace('%', '%%')), [1, 2])
cursor.execute('select %(a)s from dual', {'a': '2'})
# cursor.execute('select %(a)s from dual', a='3')
# data = cursor.fetchone()
# data = cursor.fetchmany(8)
cursor.execute('select * from {}'.format(table))
data = cursor.fetchall()
print(data)
cursor.close()
connection.commit()
connection.close()

print('\nsql_client.postgresql(punc):')
db = sql_client.postgresql.SqlClient(raise_error=True, **env.postgresql_punc)
# print(db.query('insert into {} values (1,2)'.format(table), fetchall=False))
print(db.query('insert into {} values (%s,%s)'.format(table.replace('%', '%%')), ('3', 4), fetchall=False))
print(db.save_data((5, 6), table.replace('%', '%%').replace('{', '{{').replace('}', '}}')))
print(db.query('select %(a)s from dual', {'a': '2'}))
print(db.query('select 1 from dual'))
print(db.query('select * from {}'.format(table)))

print('\nsql_client.sqlalchemy(punc):')
db = sql_client.sqlalchemy.SqlClient(dialect='postgresql', raise_error=True, **env.postgresql_punc)
# print(db.query('insert into {} values (1,2)'.format(table), fetchall=False))
print(db.query('insert into {} values (:a,:b)'.format(table), {'a': '23', 'b': '24'},
               fetchall=False))
print(db.save_data(('21', '22'), table.replace('%', '%%').replace('{', '{{').replace('}', '}}')))
print(db.query('select :a from dual', {'a': '2'}, origin_result=True))
print(db.query('select :1 from dual', {'1': 2}, origin_result=True))
print(db.query('select 1 from dual', origin_result=True))
print(db.query('select * from {}'.format(table), origin_result=True))
print(db.__dict__)
print(db.connection.get_isolation_level())
db.autocommit = False
print(db.query('insert into {} values (500,600)'.format(table), fetchall=False))
print(db.query('select * from {}'.format(table), origin_result=True))
db.try_connect()
print(db.query('select * from {}'.format(table), origin_result=True))
db = sql_client.sqlalchemy.SqlClient(dialect='postgresql', autocommit=False, raise_error=True, **env.postgresql_punc)
print(db.query('select * from {}'.format(table), origin_result=True))
print(db.query('insert into {} values (700,800)'.format(table), fetchall=False))
print(db.query('select * from {}'.format(table), origin_result=True))
db.try_connect()
print(db.query('select * from {}'.format(table), origin_result=True))
