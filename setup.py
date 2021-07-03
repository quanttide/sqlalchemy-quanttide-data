from setuptools import setup

setup(name='sql-utils', version='0.4.0', packages=['sql_utils'], url='', license='Apache 2.0', author='QuantTide Inc.',
      author_email='tech@quanttide.com', description='',
      extras_require={'sqlalchemy': ['sqlalchemy', 'tablib'], 'records': ['records'], 'mysqlclient': ['mysqlclient'],
                      'pymysql': ['pymysql'], 'mysql': ['mysqlclient'], 'postgresql': ['psycopg2'],
                      'pgsql': ['psycopg2'], 'psycopg2': ['psycopg2'], 'psycopg2-binary': ['psycopg2-binary'],
                      'sqlserver': ['pymssql'], 'oracle': ['cx_Oracle']})
