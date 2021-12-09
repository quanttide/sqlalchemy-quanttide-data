# 保存数据

保存数据推荐使用save_data方法。

前提：数据库内已建好表（以my_table为例），建议使用可视化工具操作，如Navicat。

Tips：

1. 若my_table已在建立实例时输入默认表，则以下无需输入my_table
2. MySQL若希望使用replace语句而不是insert语句，可传入statement='replace'参数（或置于第三个参数的位置）

- 第一种：数据按表的字段顺序排好，则只需列表即可，无需字典：

```python
db.save_data([[1, 'a'], [2, 'b']], 'my_table')
# 单条数据亦可不由列表包裹: db.save_data([1, 'a'], 'my_table')
```

- 第二种：自定义对应的字段，传入字典：

```python
db.save_data([{'field_1': 1, 'field_2': 'a'}, {'field_1': 2, 'field_2': 'b'}], 'my_table')
# 单条数据亦可不由列表包裹: db.save_data({'field_1': 1, 'field_2': 'a'}, 'my_table')
```

- 第三种：自定义对应的字段，传入列表：（需按key的顺序排好数据）

```python
db.save_data([['a', 1], ['b', 2]], 'my_table', keys=['field_2', 'field_1'])
# 亦可传入keys='field_2,field_1'
# 单条数据亦可不由列表包裹: db.save_data(['a', 1], 'my_table', keys=['field_2', 'field_1'])
```
