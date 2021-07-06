from setuptools import setup

setup(name='sql-client', version='0.4.0', packages=['sql_client'], url='', license='Apache 2.0',
      author='QuantTide Inc.', author_email='tech@quanttide.com', description='',
      extras_require={'sqlalchemy': ['sqlalchemy', 'tablib'], 'mysqlclient': ['mysqlclient'], 'pymysql': ['pymysql'],
                      'mysql': ['mysqlclient'], 'postgresql': ['psycopg2'], 'pgsql': ['psycopg2'],
                      'psycopg2': ['psycopg2'], 'psycopg2-binary': ['psycopg2-binary'], 'sqlserver': ['pymssql'],
                      'mssql': ['pymssql'], 'pymssql': ['pymssql'], 'oracle': ['cx_Oracle'],
                      'cx_Oracle': ['cx_Oracle']})
