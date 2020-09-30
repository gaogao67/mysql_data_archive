#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
#     FileName:
#         Desc:
#       Author:
#        Email:
#     HomePage:
#      Version:
#   LastChange:
#      History:
# =============================================================================
import os
import time
import datetime
import pymysql
import traceback
from pymysql.cursors import Cursor
from print_helper import PrintHelper
from mysql_base import MyBase
from mysql_server import MySQLServer


class MyRemoteArchive(MyBase):
    def __init__(self, source_mysql_server: MySQLServer,
                 source_table_name: str,
                 target_mysql_server: MySQLServer,
                 target_table_name: str,
                 data_condition: str,
                 batch_scan_rows: int,
                 batch_sleep_seconds,
                 is_dry_run=True,
                 is_user_confirm=True,
                 max_query_seconds=0,
                 force_table_scan=False):
        super(MyRemoteArchive, self).__init__(
            source_mysql_server=source_mysql_server,
            source_table_name=source_table_name,
            target_mysql_server=target_mysql_server,
            target_table_name=target_table_name,
            data_condition=data_condition,
            batch_scan_rows=batch_scan_rows,
            batch_sleep_seconds=batch_sleep_seconds,
            is_dry_run=is_dry_run,
            is_user_confirm=is_user_confirm,
            max_query_seconds=max_query_seconds,
            force_table_scan=force_table_scan
        )

    def get_archive_range_keys(self, current_key, next_key):
        """
        获取指定区间的主键值
        :param current_key:
        :param next_key:
        :return:
        """
        sql_script = """
SELECT `{table_primary_key}` 
FROM `{source_database_name}`.`{source_table_name}` 
WHERE {data_condition}
AND `{table_primary_key}`>='{current_key}'
AND `{table_primary_key}`<='{next_key}'
""".format(
            source_database_name=self.source_database_name,
            source_table_name=self.source_table_name,
            data_condition=self.data_condition,
            table_primary_key=self.table_primary_key,
            current_key=pymysql.escape_string(str(current_key)),
            next_key=pymysql.escape_string(str(next_key))
        )
        query_result = self.source_mysql_server.mysql_query(
            sql_script=sql_script, sql_paras=None, return_dict=True
        )
        return list(map(lambda key: key[self.table_primary_key], query_result))

    def get_data_by_keys(self, range_keys):
        """
        获取特定可以的数据
        :param range_keys:
        :return:
        """
        if len(range_keys) == 0:
            return []
        where_keys = ",".join(list(map(lambda key: "%s", range_keys)))
        sql_script = """
SELECT * 
FROM `{source_database_name}`.`{source_table_name}` 
WHERE {data_condition}
AND `{table_primary_key}` IN ({where_keys})
""".format(
            source_database_name=self.source_database_name,
            source_table_name=self.source_table_name,
            data_condition=self.data_condition,
            table_primary_key=self.table_primary_key,
            where_keys=where_keys

        )
        return self.source_mysql_server.mysql_query(
            sql_script=sql_script,
            sql_paras=range_keys,
            return_dict=True
        )

    def insert_target_range_data(self, range_data):
        if len(range_data) == 0:
            return
        row_item = range_data[0]
        row_columns = ', '.join(map(lambda key: '`{}`'.format(key), row_item.keys()))
        row_replaces = ', '.join(map(lambda key: '%s', row_item.keys()))
        sql_script = """REPLACE INTO `{}`.`{}`({})VALUES({});""".format(
            self.target_database_name,
            self.target_table_name,
            row_columns,
            row_replaces)
        script_list = []
        for row_item in range_data:
            sql_paras = list(row_item.values())
            script_list.append([sql_script, sql_paras])
        self.target_mysql_server.mysql_exec_many(script_list=script_list)

    def delete_target_data_by_keys(self, range_keys):
        if len(range_keys) == 0:
            return
        where_keys = ",".join(list(map(lambda key: "%s", range_keys)))
        sql_script = """
DELETE FROM `{target_database_name}`.`{target_table_name}` 
WHERE {data_condition}
AND `{table_primary_key}` IN ({where_keys});
""".format(
            target_database_name=self.target_database_name,
            target_table_name=self.target_table_name,
            data_condition=self.data_condition,
            table_primary_key=self.table_primary_key,
            where_keys=where_keys

        )
        self.target_mysql_server.mysql_exec(
            sql_script=sql_script,
            sql_paras=range_keys
        )

    def delete_source_data_by_keys(self, range_keys):
        if len(range_keys) == 0:
            return
        where_keys = ",".join(list(map(lambda key: "%s", range_keys)))
        sql_script = """
DELETE FROM `{source_database_name}`.`{source_table_name}` 
WHERE {data_condition}
AND `{table_primary_key}` IN ({where_keys});
""".format(
            source_database_name=self.source_database_name,
            source_table_name=self.source_table_name,
            data_condition=self.data_condition,
            table_primary_key=self.table_primary_key,
            where_keys=where_keys

        )
        self.source_mysql_server.mysql_query(
            sql_script=sql_script,
            sql_paras=range_keys,
            return_dict=True
        )

    def archive_range_data(self, current_key, next_key):
        try:
            if self.is_dry_run:
                PrintHelper.print_info_message("未真实执行，未生产SQL文件。")
                return True
            range_keys = self.get_archive_range_keys(current_key, next_key)
            range_data = self.get_data_by_keys(range_keys=range_keys)
            PrintHelper.print_info_message("找到满足条件记录{}条".format(len(range_data)))
            PrintHelper.print_info_message("开始删除目标库已有数据")
            self.delete_target_data_by_keys(range_keys=range_keys)
            PrintHelper.print_info_message("开始向目标库上插入数据")
            self.insert_target_range_data(range_data=range_data)
            PrintHelper.print_info_message("开始删除目标库归档数据")
            self.delete_source_data_by_keys(range_keys=range_keys)
            return True
        except Exception as ex:
            PrintHelper.print_warning_message("在归档过程中出现异常：{}\n堆栈：{}\n".format(str(ex), traceback.format_exc()))
            return False

    def loop_archive_data(self):
        max_key, min_key = self.get_loop_key_range()
        if max_key is None or min_key is None:
            PrintHelper.print_info_message("未找到满足条件的键区间")
            return
        current_key = min_key
        while current_key < max_key:
            if self.has_stop_file():
                break
            PrintHelper.print_info_message("*" * 70)
            next_key = self.get_next_loop_key(current_key=current_key)
            if next_key is None:
                PrintHelper.print_info_message("未找到下一个可归档的区间，退出归档")
                break
            if not self.archive_range_data(
                    current_key=current_key,
                    next_key=next_key):
                PrintHelper.print_info_message("执行出现异常，退出归档！")
                break
            current_key = next_key
            PrintHelper.print_info_message("*" * 70)
            info = """最小值为：{0},最大值为：{1},当前处理值为：{2}""".format(
                min_key, max_key, current_key)
            PrintHelper.print_info_message(info)
        PrintHelper.print_info_message("*" * 70)
        PrintHelper.print_info_message("执行完成")

    def user_confirm(self):
        info = """
将生成在服务器{mysql_host}上归档数据
迁移数据条件为:
INSERT INTO `{target_database_name}`.`{target_table_name}`
SELECT * FROM `{source_database_name}`.`{source_table_name}`
WHERE {data_condition}
"""
        info = info.format(mysql_host=self.source_mysql_server.mysql_host,
                           source_database_name=self.source_database_name,
                           source_table_name=self.source_table_name,
                           target_database_name=self.target_database_name,
                           target_table_name=self.target_table_name,
                           data_condition=self.data_condition)
        PrintHelper.print_info_message(info)
        if self.is_dry_run:
            PrintHelper.print_info_message("模拟运行。。。")
            return True
        if not self.is_user_confirm:
            return True
        input_options = ['yes', 'no']
        input_message = """
请输入yes继续或输入no退出，yes/no?
"""
        user_option = self.get_user_choose_option(
            input_options=input_options,
            input_message=input_message)
        if user_option == "no":
            return False
        else:
            return True

    def check_config(self):
        try:
            if str(self.source_database_name).strip() == "":
                PrintHelper.print_warning_message("数据库名不能为空")
                return False
            if str(self.source_table_name).strip() == "":
                PrintHelper.print_warning_message("表名不能为空")
                return False
            if str(self.data_condition).strip() == "":
                PrintHelper.print_warning_message("迁移条件不能为空")
                return False
            self.create_target_table()
            source_columns = self.get_source_columns()
            target_columns = self.get_target_columns()
            if len(source_columns) != len(target_columns):
                PrintHelper.print_warning_message("源表和目标表的列数量不匹配，不满足迁移条件")
                return False
            column_count = len(source_columns)
            primary_key_count = 0
            for column_id in range(column_count):
                source_column_name = source_columns[column_id]["Field"]
                source_column_key = source_columns[column_id]["Key"]
                source_column_type = source_columns[column_id]["Type"]
                target_column_name = target_columns[column_id]["Field"]
                target_column_key = target_columns[column_id]["Key"]
                target_column_type = source_columns[column_id]["Type"]
                if source_column_name.lower() != target_column_name.lower() \
                        or target_column_key.lower() != target_column_key.lower() \
                        or source_column_type.lower() != target_column_type.lower():
                    PrintHelper.print_warning_message("源表和目标表的列不匹配，不满足迁移条件")
                    return False
                if source_column_key.lower() == 'pri':
                    primary_key_count += 1
                    self.table_primary_key = source_column_name
                    if 'int(' in str(source_column_type).lower() \
                            or 'bigint(' in str(source_column_type).lower():
                        self.table_primary_key_type = 'INT'
                    elif 'varchar(' in str(source_column_type).lower():
                        self.table_primary_key_type = 'CHAR'
            if self.table_primary_key_type == "":
                PrintHelper.print_warning_message("主键不为int/bigint/varchar三种类型中的一种，不满足迁移条件")
                return False
            if primary_key_count == 0:
                PrintHelper.print_warning_message("未找到主键，不瞒足迁移条件")
                return False
            if primary_key_count > 1:
                PrintHelper.print_warning_message("要迁移的表使用复合主键，不满足迁移条件")
                return False
            return True
        except Exception as ex:
            PrintHelper.print_warning_message("执行出现异常，异常为{0},{1}".format(
                str(ex), traceback.format_exc()))
            return False

    def archive_data(self):
        PrintHelper.print_info_message("开始检查归档配置")
        if not self.check_config():
            return
        confirm_result = self.user_confirm()
        if not confirm_result:
            return
        PrintHelper.print_info_message("开始归档数据")
        self.loop_archive_data()
        PrintHelper.print_info_message("结束归档数据")

    def get_source_table_create_script(self):
        """
        获取特定表的建表语句
        :param database_name:
        :param table_name:
        :return:
        """
        sql_script = "show create table `{}`.`{}`".format(
            self.source_database_name,
            self.source_table_name
        )
        query_result = self.source_mysql_server.mysql_query(
            sql_script=sql_script,
            return_dict=False
        )
        if len(query_result) > 0:
            return query_result[0][1]
        else:
            return None

    def get_target_table_create_script(self):
        """
        获取目标表的建表语句
        :return:
        """
        source_script = self.get_source_table_create_script()
        return str(source_script).replace(
            "CREATE TABLE `{}`".format(self.source_table_name),
            "CREATE TABLE `{}`.`{}`".format(self.target_database_name, self.target_table_name),
        )

    def create_target_table(self):
        if not self.is_target_table_exist():
            PrintHelper.print_info_message("准备创建目标表")
            table_script = self.get_target_table_create_script()
            self.target_mysql_server.mysql_exec(sql_script=table_script)
        else:
            PrintHelper.print_info_message("目标表已存在")
