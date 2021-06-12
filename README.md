# sql-utils

SQL数据库的Python操作库的封装。

## 维护者

- Owner：黄日航（huangrihang@quanttide.com）
- Reviewer：张果（zhangguo@quanttide.com）
- 创建日期：2021-06-03
- 更新日期：2021-06-13
- 代码版本：0.3.0

## 快速入门

base_sql_util.py是基础文件，其余每个xxx_util.py文件对应一个被封装的第三方库。

推荐使用sqlalchemy_util.py，以下均以sqlalchemy_util.py为例。

### 引入文件

只需将base_sql_util.py, sqlalchemy_util.py这两个文件放入项目目录或云函数的层，然后import：（推荐像这样直接导入SqlUtil类，这样若后续需更换其它util.py文件时只需修改import语句）

```python
from sqlalchemy_util import SqlUtil
```

### 建立实例(并自动连接)

实例化SqlUtil类（以postgresql为例），建议放于全局变量以复用连接。

```python
DB = SqlUtil(dialect='postgresql', host='...', port=..., user='...', password='...', database='...')
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

可传入dictionary=True/False参数，控制结果以字典或列表格式输出。（sqlalchemy_util & records_util特有：传入dataset=True参数，结果以tablib.Dataset类输出）

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
# 仅sqlalchemy_util & records_util此种情况需传入not_one_by_one=False，以支持%s填充
```

```python
DB.query('update my_table set field_2=%(field_2)s where field_1=%(field_1)s', {'field_1': 1, 'field_2': 'a'}, keep_args_as_dict=True)
```

```python
DB.query('update my_table set field_2=%(field_2)s where field_1=%(field_1)s', [{'field_1': 1, 'field_2': 'a'}, {'field_1': 2, 'field_2': 'b'}], keep_args_as_dict=True)
```

#### 选取未处理的数据并标记处理中

开启事务，选取一条is_tried=0的数据，将is_tried置为1（通过key_fields定位，建议传入主键），提交事务并返回key_fields + extra_fields的内容。（若选取不到符合条件数据或事务执行出错，则返回空数据）

**注意：需提前将事务隔离级别设为REPEATABLE READ。**（MySQL初始为REPEATABLE READ，但PostgreSQL初始不是）

Tips：1. 若my_table已在建立实例时输入默认表，则以下无需输入my_table；2. 其它主要参数默认值：num=1(选取1条数据), tried=0, tried_after=1, finished=None(不启用), finished_field='is_finished'；3. 可传入dictionary=True/False参数，控制结果以字典或列表格式输出。（sqlalchemy_util & records_util特有：传入dataset=True参数，结果以tablib.Dataset类输出）

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

