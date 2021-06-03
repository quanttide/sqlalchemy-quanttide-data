import cx_Oracle
import oracle_util
import records
import env

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

database = oracle_util.SqlUtil(**env.oracle)
# print(database.query('insert into test values (1,2)', fetchall=False))
print(database.query('insert into test values (:1,:2)', (3,4), fetchall=False))
print(database.save_data((5,6), 'test'))
print(database.query('select :a from dual', {'a': 2}, keep_args_as_dict=True))
print(database.query('select :a from dual', [2]))
print(database.query('select 1 from dual'))
print(database.query('select * from test'))

connection = records.Database('oracle://{user}:{password}@{host}:{port}/{database}'.format(
    **env.oracle)).get_connection()
print(connection.bulk_query('insert into test values (7,8)'))
print(connection.bulk_query('insert into test values (:a,:b)', {'a':9,'b':10}))
print(connection.bulk_query('insert into test values (:a,:b)', ({'a':11,'b':12},)))
print(connection.bulk_query('insert into test values (:1,:2)', [{'1':13,'2':14}]))
print(connection.query('select :a from dual', **{'a': '2'}).all())
print(connection.query('select :a from dual', a='3').all())
print(connection.query('select 1 from dual').all())
print(connection.query('select * from test').all())
