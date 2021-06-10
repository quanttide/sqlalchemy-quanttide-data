import psycopg2
import records

import postgresql_util
import sqlalchemy_util
import records_util
import env

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

database = postgresql_util.SqlUtil(**env.postgresql)
# print(database.query('insert into t values (1,2)', fetchall=False))
print(database.query('insert into t values (%s,%s)', ('3', 4), fetchall=False))
print(database.save_data((5, 6), 't'))
print(database.query('select %(a)s from dual', {'a': '2'}, keep_args_as_dict=True))
print(database.query('select 1 from dual'))
print(database.query('select * from t'))

connection = records.Database('postgresql://{user}:{password}@{host}:{port}/{database}'.format(
    **env.postgresql)).get_connection()
connection.bulk_query('insert into t values (7,8)')
connection.bulk_query('insert into t values (:a,:b)', {'a': 9, 'b': 10})
connection.bulk_query('insert into t values (:a,:b)', ({'a': 11, 'b': 12},))
connection.bulk_query('insert into t values (:1,:2)', [{'1': 13, '2': 14}])
# print(connection.query('insert into t values (15,16)').all())
# print(connection.query('insert into t values (:a,:b)', **{'a': 19, 'b': 20}).all())
# print(connection.query('insert into t values (:1,:2)', **{'1': 17, '2': 28}).all())
print(connection.query('select :a from dual', **{'a': '2'}).all())
print(connection.query('select :a from dual', a='3').all())
print(connection.query('select 1 from dual').all())
print(connection.query('select * from t').all())
print(connection.query('select 1 from dual').dataset)
print(connection.query('select * from t').dataset)

database = records_util.SqlUtil(dialect='postgresql', **env.postgresql)
# print(database.query('insert into t values (1,2)', fetchall=False))
print(database.query('insert into t values (:a,:b)', {'a': '23', 'b': '24'}, keep_args_as_dict=True, fetchall=False))
print(database.save_data(('21', '22'), 't'))
print(database.query('select :a from dual', {'a': '2'}, keep_args_as_dict=True, origin_result=True))
print(database.query('select 1 from dual', origin_result=True))
print(database.query('select * from t', origin_result=True))
print(database.__dict__)
print(database.connection._conn.get_isolation_level())
database.autocommit = False
print(database.query('insert into t values (100,200)', fetchall=False))
print(database.query('select * from t', origin_result=True))
database.try_connect()
print(database.query('select * from t', origin_result=True))
database = records_util.SqlUtil(dialect='postgresql', autocommit=False, **env.postgresql)
print(database.query('select * from t', origin_result=True))
print(database.query('insert into t values (300,400)', fetchall=False))
print(database.query('select * from t', origin_result=True))
database.try_connect()
print(database.query('select * from t', origin_result=True))

database = sqlalchemy_util.SqlUtil(dialect='postgresql', **env.postgresql)
# print(database.query('insert into t values (1,2)', fetchall=False))
print(database.query('insert into t values (:a,:b)', {'a': '23', 'b': '24'}, keep_args_as_dict=True, fetchall=False))
print(database.save_data(('21', '22'), 't'))
print(database.query('select :a from dual', {'a': '2'}, keep_args_as_dict=True, origin_result=True))
print(database.query('select 1 from dual', origin_result=True))
print(database.query('select * from t', origin_result=True))
print(database.__dict__)
print(database.connection.get_isolation_level())
database.autocommit = False
print(database.query('insert into t values (500,600)', fetchall=False))
print(database.query('select * from t', origin_result=True))
database.try_connect()
print(database.query('select * from t', origin_result=True))
database = sqlalchemy_util.SqlUtil(dialect='postgresql', autocommit=False, **env.postgresql)
print(database.query('select * from t', origin_result=True))
print(database.query('insert into t values (700,800)', fetchall=False))
print(database.query('select * from t', origin_result=True))
database.try_connect()
print(database.query('select * from t', origin_result=True))

# ------分割线------
table = '"' + '!"#$%&\'()*+,-./:;<=>?@[\]^_`{|}~t '.replace('"', '""') + '"'

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

database = postgresql_util.SqlUtil(**env.postgresql_punc)
# print(database.query('insert into {} values (1,2)'.format(table), fetchall=False))
print(database.query('insert into {} values (%s,%s)'.format(table.replace('%', '%%')), ('3', 4), fetchall=False))
print(database.save_data((5, 6), table.replace('%', '%%').replace('{', '{{').replace('}', '}}')))
print(database.query('select %(a)s from dual', {'a': '2'}, keep_args_as_dict=True))
print(database.query('select 1 from dual'))
print(database.query('select * from {}'.format(table)))

connection = records.Database('postgresql://{}:{}@{}:{}/{}'.format(
    env.postgresql_punc['user'].replace(':', '%3A').replace('/', '%2F'),
    env.postgresql_punc['password'].replace('@', '%40'), env.postgresql_punc['host'], env.postgresql_punc['port'],
    env.postgresql_punc['database'])).get_connection()
connection.bulk_query('insert into {} values (7,8)'.format(table))
connection.bulk_query('insert into {} values (:a,:b)'.format(table), {'a': 9, 'b': 10})
connection.bulk_query('insert into {} values (:a,:b)'.format(table), ({'a': 11, 'b': 12},))
connection.bulk_query('insert into {} values (:1,:2)'.format(table), [{'1': 13, '2': 14}])
# print(connection.query('insert into {} values (15,16)'.format(table)).all())
# print(connection.query('insert into {} values (:a,:b)'.format(table), **{'a': 19, 'b': 20}).all())
# print(connection.query('insert into {} values (:1,:2)'.format(table), **{'1': 17, '2': 28}).all())
print(connection.query('select :a from dual', **{'a': '2'}).all())
print(connection.query('select :a from dual', a='3').all())
print(connection.query('select 1 from dual').all())
print(connection.query('select * from {}'.format(table)).all())
print(connection.query('select 1 from dual').dataset)
print(connection.query('select * from {}'.format(table)).dataset)

database = records_util.SqlUtil(dialect='postgresql', **env.postgresql_punc)
# print(database.query('insert into {} values (1,2)'.format(table), fetchall=False))
print(database.query('insert into {} values (:a,:b)'.format(table), {'a': '23', 'b': '24'}, keep_args_as_dict=True,
                     fetchall=False))
print(database.save_data(('21', '22'), table.replace('%', '%%').replace('{', '{{').replace('}', '}}')))
print(database.query('select :a from dual', {'a': '2'}, keep_args_as_dict=True, origin_result=True))
print(database.query('select 1 from dual', origin_result=True))
print(database.query('select * from {}'.format(table), origin_result=True))
print(database.__dict__)
print(database.connection._conn.get_isolation_level())
database.autocommit = False
print(database.query('insert into {} values (100,200)'.format(table), fetchall=False))
print(database.query('select * from {}'.format(table), origin_result=True))
database.try_connect()
print(database.query('select * from {}'.format(table), origin_result=True))
database = records_util.SqlUtil(dialect='postgresql', autocommit=False, **env.postgresql_punc)
print(database.query('select * from {}'.format(table), origin_result=True))
print(database.query('insert into {} values (300,400)'.format(table), fetchall=False))
print(database.query('select * from {}'.format(table), origin_result=True))
database.try_connect()
print(database.query('select * from {}'.format(table), origin_result=True))

database = sqlalchemy_util.SqlUtil(dialect='postgresql', **env.postgresql_punc)
# print(database.query('insert into {} values (1,2)'.format(table), fetchall=False))
print(database.query('insert into {} values (:a,:b)'.format(table), {'a': '23', 'b': '24'}, keep_args_as_dict=True,
                     fetchall=False))
print(database.save_data(('21', '22'), table.replace('%', '%%').replace('{', '{{').replace('}', '}}')))
print(database.query('select :a from dual', {'a': '2'}, keep_args_as_dict=True, origin_result=True))
print(database.query('select 1 from dual', origin_result=True))
print(database.query('select * from {}'.format(table), origin_result=True))
print(database.__dict__)
print(database.connection.get_isolation_level())
database.autocommit = False
print(database.query('insert into {} values (500,600)'.format(table), fetchall=False))
print(database.query('select * from {}'.format(table), origin_result=True))
database.try_connect()
print(database.query('select * from {}'.format(table), origin_result=True))
database = sqlalchemy_util.SqlUtil(dialect='postgresql', autocommit=False, **env.postgresql_punc)
print(database.query('select * from {}'.format(table), origin_result=True))
print(database.query('insert into {} values (700,800)'.format(table), fetchall=False))
print(database.query('select * from {}'.format(table), origin_result=True))
database.try_connect()
print(database.query('select * from {}'.format(table), origin_result=True))
