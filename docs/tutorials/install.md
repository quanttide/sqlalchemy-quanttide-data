# 下载与安装

## pip安装

- 第一种：安装对应模块依赖：（推荐）

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git sqlalchemy tablib
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git sqlalchemy tablib mysqlclient
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git mysqlclient
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git pymysql
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git psycopg2
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git pymssql
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git cx_Oracle
```

- 第二种：仅安装本库，自行安装依赖：

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git
```

Tips：

1. 腾讯云云函数已内置依赖：[](https://cloud.tencent.com/document/product/583/55592)

   包含：mysqlclient, PyMySQL, psycopg2-binary（import名仍为psycopg2，只是从pip角度为与psycopg2不相关的两个包）

### 依赖

各模块依赖：

- sql_client.sqlalchemy 依赖 sqlalchemy, tablib（以及对应引擎库）
- sql_client.mysqlclient 依赖 mysqlclient
- sql_client.pymysql 依赖 pymysql
- sql_client.postgresql 依赖 psycopg2
- sql_client.sqlserver 依赖 pymssql
- sql_client.oracle 依赖 cx_Oracle

### 另一种方式：直接引入文件

以sql_client.sqlalchemy为例，只需将sql_client/base.py, sql_client/sqlalchemy.py这两个文件放入项目目录或云函数的层即可。
