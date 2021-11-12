# 任务调度

sql-client库提供了开箱即用的基于SQL表的任务调度，方便开发者处理海量并发任务时快速接入调度逻辑。

## 选取未处理的数据并标记处理中

开启事务，选取一条或多条数据（默认加锁），update指定字段（通过key_fields定位记录，建议key_fields传入主键或唯一标识字段），提交事务并返回key_fields + extra_fields的内容。（若选取不到符合条件数据或事务执行出错，则返回空数据）

Tips：

1. 若my_table已在建立实例时输入默认表，则以下无需输入my_table
2. 其它主要参数默认值：num=1(选取1条数据), key_fields='id', tried='between'(选取>=tried_min <=tried_max), tried_min=1, tried_max=5, tried_after='-'(取相反数), next_time=None(<=当前时间), next_time_after=()(不修改), lock=True
3. tried_field, finished_field, next_time_field字段传入与否分别决定相关逻辑启用与否
4. 数据库中数据建议tried_field字段初始值为1, next_time_field字段初始值为0(使用默认参数无法选择到为null的记录)
5. 可传入dictionary=True/False参数，控制结果以字典或列表格式输出（sql_client.sqlalchemy特有：传入dataset=True参数，结果以tablib.Dataset类输出）

```python
data = db.select_to_try('my_table', key_fields='field_1', extra_fields='field_2', tried_field='round_num', next_time_field='next_time')
# key_fields和extra_fields若为多个字段，传入'field_1,field_2'或['field_1', 'field_2']均可
```

## 标记处理结束

通过result(第一个参数)与key_fields定位（建议传入主键或唯一标识字段）选取数据，update指定字段，返回1(执行成功)或0(执行失败)。

若result传入列表而不是字典，则必须传入key_fields参数。

参数默认值为成功情形；针对失败情形，可使用fail_try方法，参数和逻辑与end_try一致，仅默认值不同：tried='-+1'(取相反数加一), next_time=300(当前时间+300秒)。

针对提前取消处理的情形，可使用cancel_try方法，参数和逻辑与end_try一致，仅默认值不同：tried='-'(取相反数，即恢复原状)。

Tips：

1. 若my_table已在建立实例时输入默认表，则以下无需输入my_table
2. 其它主要参数默认值：tried=0, next_time='=0'
3. tried_field, finished_field, next_time_field字段传入与否分别决定相关逻辑启用与否

```python
db.end_try([{'field_1': 1, 'field_2': 'a'}, {'field_1': 2, 'field_2': 'b'}], 'my_table', tried_field='round_num', next_time_field='next_time')
# 单条数据亦可不由列表包裹: db.end_try({'field_1': 1, 'field_2': 'a'}, 'my_table')
```

```python
db.end_try([[1, 'a'], [2, 'b']], 'my_table', key_fields=['field_1', 'field_2'], tried_field='round_num', next_time_field='next_time')
# 亦可传入key_fields='field_1,field_2'
# 单条数据亦可不由列表包裹: db.end_try([1, 'a'], 'my_table', key_fields=['field_1', 'field_2'])
```

```python
db.end_try([[1], [2]], 'my_table', key_fields='field_1', tried_field='round_num', next_time_field='next_time')
# 单条数据亦可不由列表包裹: db.end_try([1], 'my_table') 或 db.end_try(1, 'my_table')
```
