# CHANGELOG

## [0.3.0] - 2021-06-12

### 变更(Changed)

- query, save_data方法增加keys参数：增加自行传入auto_format模式的keys的功能，传入keys时强制开启auto_format模式
- save_data方法删除auto_format参数：原版本若传入auto_format=False实际并不能达到预期效果，故save_data方法现强制开启auto_format模式
- \_\_init\_\_方法dictionary参数默认值改为False
- select_to_try方法finished, autocommit_after参数默认值改为None
- select_to_try方法增加dictionary, origin_result, dataset参数
- records_util与sqlalchemy_util参数优先级改为origin_result > dataset > dictionary

## [0.2.1] - 2021-06-10

### 变更(Changed)

- sqlalchemy_util, records_util 现在可以对实例化时传入的 user, password 参数中影响sqlalchemy解析url的特殊字符自动进行转义（tips: 若从dialect或环境变量传入整个url，需先自行转义）

### 优化(Refactored)

- 清理冗余逻辑，修复代码格式问题

## [0.2.0] - 2021-06-06

### 新增(Features)

- 增加finish方法

### 变更(Changed)

- 大幅增加可在实例化时设默认值的通用参数数量
- 所有主要API增加多个参数，原有参数微调
- 较多API实现逻辑与效果微调
- select_to_try方法增加plus_1功能

### 优化(Refactored)

- lib属性由实例属性改为类属性
- standardize_args方法不再为staticmethod
- 弃用_auto_format_query方法

## [0.1.1] - 2021-06-06

### 修复(Fixed)

- 修复oracle_util对dictionary参数效果实现的错误逻辑

### 优化(Refactored)

- 修改方法定义顺序，常用方法前置

## [0.1.0] - 2021-06-03

### 新增(Features)

- 初始提交（可用版本）
- 支持sqlalchemy, records, mysqlclient, pymysql, psycopg2 (postgresql), pymssql (sqlserver), cx_Oracle (oracle)

