# -*- coding: utf-8 -*-

import sys

sys.path.append('..')

import cx_Oracle

import sql_client.oracle
import sql_client.sqlalchemy
import env

print('cx_Oracle:')
connection = cx_Oracle.connect(user=env.oracle['user'], password=env.oracle['password'],
                               dsn='{host}:{port}/{database}'.format(**env.oracle), encoding='utf8')
connection.autocommit = True
cursor = connection.cursor()
cursor.execute('delete from test')
cursor.execute('insert into test values (1,2)', ())
# cursor.callproc(None)
# cursor.callfunc('None',int,())
# cursor.executemany('insert into test values (1,2)', 2)
# cursor.execute('select :a from dual', {'a': '2'})
cursor.execute('select :a from dual', [2])
# cursor.execute('select :a from dual', a='3')
# data = cursor.fetchone()
# data = cursor.fetchmany(8)
cursor.execute('select * from test')
data = cursor.fetchall()
print(data)
cursor.close()
connection.commit()
connection.close()

print('\nsql_client.postgresql:')
db = sql_client.oracle.SqlClient(**env.oracle)
# print(database.query('insert into test values (1,2)', fetchall=False))
print(db.query('insert into test values (:1,:2)', (3, 4), fetchall=False))
print(db.save_data((5, 6), 'test'))
print(db.query('select :a from dual', {'a': 2}))
print(db.query('select :a from dual', [2]))
print(db.query('select 1 from dual'))
print(db.query('select * from test'))
print(db.query('select :a from dual', [2], dictionary=True))
print(db.query('select 1 from dual', dictionary=True))
print(db.query('select * from test', dictionary=True))

print('\nsql_client.sqlalchemy:')
db = sql_client.sqlalchemy.SqlClient(raise_error=True, **env.oracle)
# print(database.query('insert into test values (1,2)', fetchall=False))
print(db.query('insert into test values (:1,:2)', (15, 16), fetchall=False))
print(db.save_data((17, 18), 'test'))
print(db.query('select :a from dual', {'a': 2}))
print(db.query('select :a from dual', [2]))
print(db.query('select 1 from dual'))
print(db.query('select * from test'))
print(db.query('select :a from dual', [2], dictionary=True))
print(db.query('select 1 from dual', dictionary=True))
print(db.query('select * from test', dictionary=True))
