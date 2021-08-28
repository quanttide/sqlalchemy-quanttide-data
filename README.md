# sql-client

SQL数据库的Python操作库的封装。

## 特性

- 封装各个SQL数据库的Python操作库，提供基本一致的API，只需更换import语句（以及其它一些小调整）即可更换底层库甚至更换数据库类型（如从MySQL更换为PostgreSQL）

## 下载安装

### pip安装

- 第一种：安装对应模块依赖：（推荐）

Tips：1. 如需一次性安装多个，在中括号内用逗号隔开即可。2. 支持别名：mysql->mysqlclient，pgsql->psycopg2，psycopg2->psycopg2，psycopg2-binary->psycopg2-binary。

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git[sqlalchemy]
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git[sqlalchemy,mysqlclient]
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git[mysqlclient]
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git[pymysql]
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git[postgresql]
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git[sqlserver]
```

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git[oracle]
```

- 第二种：仅安装本库，自行安装依赖：

```shell
pip install git+https://e.coding.net/quanttide/serverless-bigdata/sql-client.git
```

Tips：腾讯云云函数已内置依赖：[](https://cloud.tencent.com/document/product/583/55592)

​	包含：mysqlclient, PyMySQL, psycopg2-binary（import名仍为psycopg2，只是从pip角度为与psycopg2不相关的两个包）

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


## 快速入门

sql_client/base.py是基础文件，sql_client目录下其余每个模块对应一个被封装的第三方库。

推荐使用sql_client.sqlalchemy，以下均以sql_client.sqlalchemy为例。

### 导入模块

推荐像这样直接导入SqlClient类，这样若后续需更换其它模块时只需修改import语句：

```python
from sql_client.sqlalchemy import SqlClient
```

### 建立实例(并自动连接)

实例化SqlClient类（以postgresql为例），建议放于全局变量以复用连接。

```python
DB = SqlClient(dialect='postgresql', host='...', port=..., user='...', password='...', database='...')
# 亦可传入table='...'参数作为全局的默认表
# 如希望query方法返回的结果默认为字典格式，可传入dictionary=True
```

### 数据库操作

#### 保存数据

保存数据推荐使用save_data方法。

前提：数据库内已建好表（以my_table为例），建议使用可视化工具操作，如Navicat。

Tips：1. 若my_table已在建立实例时输入默认表，则以下无需输入my_table；2. MySQL若希望使用replace语句而不是insert语句，可传入statement='replace'参数（或置于第三个参数的位置）。

- 第一种：数据按表的字段顺序排好，则只需列表即可，无需字典：

```python
DB.save_data([[1, 'a'], [2, 'b']], 'my_table')
# 单条数据亦可不由列表包裹: DB.save_data([1, 'a'], 'my_table')
```

- 第二种：自定义对应的字段，传入字典：

```python
DB.save_data([{'field_1': 1, 'field_2': 'a'}, {'field_1': 2, 'field_2': 'b'}], 'my_table')
# 单条数据亦可不由列表包裹: DB.save_data({'field_1': 1, 'field_2': 'a'}, 'my_table')
```

- 第三种：自定义对应的字段，传入列表：（需按key的顺序排好数据）

```python
DB.save_data([['a', 1], ['b', 2]], 'my_table', keys=['field_2', 'field_1'])
# 亦可传入keys='field_2,field_1'
# 单条数据亦可不由列表包裹: DB.save_data(['a', 1], 'my_table', keys=['field_2', 'field_1'])
```

#### 查询数据/执行自定义SQL语句

查询数据或执行自定义SQL语句均使用query方法。

可传入dictionary=True/False参数，控制结果以字典或列表格式输出。（sql_client.sqlalchemy特有：传入dataset=True参数，结果以tablib.Dataset类输出）

可传入fetchall=False参数，屏蔽SQL语句的执行结果，return成功执行的数据条数。

```python
DB.query('select * from my_table')
```

```python
DB.query('select field_1,field_2 from my_table')
```

```python
DB.query('update my_table set field_2=%s where field_1=%s', ['a', 1])
```

```python
DB.query('update my_table set field_2=%s where field_1=%s', [['a', 1], ['b', 2]], not_one_by_one=False)
# 仅sql_client.sqlalchemy此种情况需传入not_one_by_one=False，以支持%s填充
```

```python
DB.query('update my_table set field_2=:field_2 where field_1=:field_1', {'field_1': 1, 'field_2': 'a'})
```

```python
DB.query('update my_table set field_2=:field_2 where field_1=:field_1', [{'field_1': 1, 'field_2': 'a'}, {'field_1': 2, 'field_2': 'b'}])
```

#### 选取未处理的数据并标记处理中

开启事务，选取一条is_tried=0的数据，将is_tried置为1（通过key_fields定位，建议传入主键），提交事务并返回key_fields + extra_fields的内容。（若选取不到符合条件数据或事务执行出错，则返回空数据）

**注意：需提前将事务隔离级别设为REPEATABLE READ。**（MySQL初始为REPEATABLE READ，但PostgreSQL初始不是）

Tips：1. 若my_table已在建立实例时输入默认表，则以下无需输入my_table；2. 其它主要参数默认值：num=1(选取1条数据), tried=0, tried_after=1, finished=None(不启用), finished_field='is_finished'；3. 可传入dictionary=True/False参数，控制结果以字典或列表格式输出。（sql_client.sqlalchemy特有：传入dataset=True参数，结果以tablib.Dataset类输出）

```python
data = DB.select_to_try('my_table', key_fields='field_1', extra_fields='field_2')
```

#### 标记处理完成

通过result(第一个参数)与key_fields定位（建议传入主键）选取数据，将is_finished置为1，返回1(执行成功)或0(执行失败)。

若result传入列表而不是字典，则必须传入key_fields参数。

Tips：1. 若my_table已在建立实例时输入默认表，则以下无需输入my_table；2. 其它主要参数默认值：finished=1, finished_field='is_finished'。

```python
DB.finish([{'field_1': 1, 'field_2': 'a'}, {'field_1': 2, 'field_2': 'b'}], 'my_table')
# 单条数据亦可不由列表包裹: DB.finish({'field_1': 1, 'field_2': 'a'}, 'my_table')
```

```python
DB.finish([[1, 'a'], [2, 'b']], 'my_table', key_fields=['field_1', 'field_2'])
# 亦可传入key_fields='field_1,field_2'
# 单条数据亦可不由列表包裹: DB.finish([1, 'a'], 'my_table', key_fields=['field_1', 'field_2'])
```

```python
DB.finish([[1], [2]], 'my_table', key_fields='field_1')
# 单条数据亦可不由列表包裹: DB.finish([1], 'my_table') 或 DB.finish(1, 'my_table')
```

## 更新日志

[CHANGELOG](CHANGELOG)

## 开源协议

[Apache 2.0](LICENSE)


## 贡献者

- 黄日航（huangrihang@quanttide.com）
- 张果（zhangguo@quanttide.com）
