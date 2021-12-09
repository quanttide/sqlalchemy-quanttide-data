# 连接数据库

sql_client/base.py是基础文件，sql_client目录下其余每个模块对应一个被封装的第三方库。

推荐使用sql_client.sqlalchemy，以下均以sql_client.sqlalchemy为例。

## 导入模块

推荐像这样直接导入SqlClient类，这样若后续需更换其它模块时只需修改import语句：

```python
from sql_client.sqlalchemy import SqlClient
```

## 建立实例(并自动连接)

实例化SqlClient类。（以postgresql为例）

数据库信息相关未传入时会自动从以下相应环境变量中读取：DB_DIALECT, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE, DB_TABLE

Tips：

1. dialect：数据库类型，仅sql_client.sqlalchemy模块使用，支持：mysql, postgresql, sqlite, oracle, mssql(支持别名：sqlserver), firebird, sybase

```python
db = SqlClient(dialect='postgresql', host='...', port=..., user='...', password='...', database='...')
# 亦可传入table='...'参数作为全局的默认表
# 如希望query方法返回的结果默认为字典格式，可传入dictionary=True
```

```python
db = SqlClient()
# 数据库信息置于环境变量中
```

## 查询数据/执行自定义SQL语句

查询数据或执行自定义SQL语句均使用query方法。

可传入dictionary=True/False参数，控制结果以字典或列表格式输出。（sql_client.sqlalchemy特有：传入dataset=True参数，结果以tablib.Dataset类输出）

可传入fetchall=False参数，屏蔽SQL语句的执行结果，return成功执行的数据条数。

```python
db.query('select * from my_table')
```

```python
db.query('select field_1,field_2 from my_table')
```

```python
db.query('update my_table set field_2=%s where field_1=%s', ['a', 1])
```

```python
db.query('update my_table set field_2=%s where field_1=%s', [['a', 1], ['b', 2]], not_one_by_one=False)
# 仅sql_client.sqlalchemy此种情况需传入not_one_by_one=False，以支持%s填充
```

```python
db.query('update my_table set field_2=:field_2 where field_1=:field_1', {'field_1': 1, 'field_2': 'a'})
```

```python
db.query('update my_table set field_2=:field_2 where field_1=:field_1', [{'field_1': 1, 'field_2': 'a'}, {'field_1': 2, 'field_2': 'b'}])
```