"""
sudo apt-get install libmysqlclient-dev
pip3 install mysqlclient
"""
import MySQLdb

import base_sql_util


class SqlUtil(base_sql_util.SqlUtil):
    def __init__(self, host, port=3306, user=None, password=None, database=None, charset='utf8mb4', autocommit=True,
                 connect_now=True, dictionary=True, log=True):
        self.lib = MySQLdb
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, dictionary, log)
