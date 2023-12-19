"""
临时工具函数模块
"""


def get_key_fields(key_fields, result):
    # 如果 key_fields 未提供（为 None 或空），则通过获取 result 中第一行的键（字段名）来设置默认值。
    # 这里假设 result 是一个字典或包含字典的列表，其中每个字典表示一行记录。
    if not key_fields:
        key_fields = tuple(result[0].keys())
    # 如果 key_fields 被提供并且是字符串类型，那么假定它是用逗号分隔的字段名称字符串。
    # 代码会将其拆分成字段名称的列表，并去除每个字段名称两端的空格。
    elif isinstance(key_fields, str):
        key_fields = [key.strip() for key in key_fields.split(',')]
    return key_fields


def get_update_tried(tried_field, tried):
    flag = 0
    if not tried_field:
        update_tried = ''
    elif tried == '-+1':
        update_tried = '{0}=-{0}+1'.format(tried_field)
    elif tried == '-':
        update_tried = '{0}=-{0}'.format(tried_field)
    elif tried == '+1':
        update_tried = '{0}={0}+1'.format(tried_field)
    elif isinstance(tried, str) and tried.lstrip().startswith('='):
        update_tried = tried_field + tried
    elif isinstance(tried, int):
        update_tried = '{}={}'.format(tried_field, tried)
    else:
        update_tried = tried_field + '=%s'
        flag = 1
    return update_tried, flag


def get_update_finished(finished_field, finished):
    flag = 0
    # `update_finished`参数
    if not finished_field:
        update_finished = ''
    elif isinstance(finished, str) and finished.lstrip().startswith('='):
        update_finished = finished_field + finished
    elif isinstance(finished, int):
        update_finished = '{}={}'.format(finished_field, finished)
    else:
        update_finished = finished_field + '=%s'
        flag = 1
    return update_finished, flag


def get_update_next_time(next_time_field, next_time):
    flag = 0
    if not next_time_field:
        update_next_time = ''
    elif isinstance(next_time, (int, float)):
        update_next_time = '{}={}'.format(next_time_field,
                                          int(time.time()) + next_time if next_time < 10 ** 9 else next_time)
    elif isinstance(next_time, str) and next_time.lstrip().startswith('='):
        update_next_time = next_time_field + next_time
    else:
        update_next_time = next_time_field + '=%s'
        flag = 1
    return update_next_time


def get_update_where(update_where, key_fields, result):
    flag = 0
    if update_where is None:
        update_where = ' or '.join((' and '.join(map('{}=%s'.format, key_fields)),) * len(result))
        if isinstance(result[0], dict):
            flag = 1
        else:
            flag = 2
    elif update_where.startswith('where'):
        update_where = update_where[5:].lstrip(' ')
    elif update_where.startswith(' where'):
        update_where = update_where[6:].lstrip(' ')
    return update_where, flag
