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
from print_helper import PrintHelper
from mysql_base import MyBase


class MyArchive(MyBase):
    def __init__(self, mysql_server,
                 source_database_name,
                 source_table_name,
                 target_database_name,
                 target_table_name,
                 data_condition,
                 batch_scan_rows,
                 batch_sleep_seconds,
                 is_dry_run=True,
                 is_user_confirm=True,
                 max_query_seconds=0):
        super(MyArchive, self).__init__(
            mysql_server=mysql_server,
            source_database_name=source_database_name,
            source_table_name=source_table_name,
            data_condition=data_condition,
            batch_scan_rows=batch_scan_rows,
            batch_sleep_seconds=batch_sleep_seconds,
            is_dry_run=is_dry_run,
            is_user_confirm=is_user_confirm,
            max_query_seconds=max_query_seconds
        )
        self.target_database_name = target_database_name
        self.target_table_name = target_table_name

    def get_archive_script_int(self, min_key_value, max_key_value):
        insert_script = """
INSERT INTO `{target_database_name}`.`{target_table_name}`
SELECT * FROM `{source_database_name}`.`{source_table_name}`
WHERE `{table_primary_key}`>={min_key_value}
AND `{table_primary_key}`<{max_key_value}
AND {data_condition} ;
""".format(target_database_name=self.target_database_name,
           source_database_name=self.source_database_name,
           target_table_name=self.target_table_name,
           source_table_name=self.source_table_name,
           min_key_value=min_key_value,
           max_key_value=max_key_value,
           data_condition=self.data_condition,
           table_primary_key=self.table_primary_key)
        delete_script = """
DELETE FROM  `{source_database_name}`.`{source_table_name}`
WHERE `{table_primary_key}` IN (
SELECT `{table_primary_key}`
FROM `{target_database_name}`.`{target_table_name}`
WHERE `{table_primary_key}`>='{min_key_value}'
AND `{table_primary_key}`<='{max_key_value}')
AND `{table_primary_key}`>='{min_key_value}'
AND `{table_primary_key}`<='{max_key_value}'
AND {data_condition};
""".format(target_database_name=self.target_database_name,
           target_table_name=self.target_table_name,
           source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
           min_key_value=min_key_value,
           max_key_value=max_key_value,
           data_condition=self.data_condition,
           table_primary_key=self.table_primary_key)
        return {"insert_script": insert_script, "delete_script": delete_script}

    def get_archive_script_char(self, min_key_value, max_key_value):
        insert_script = """
INSERT INTO `{target_database_name}`.`{target_table_name}`
SELECT * FROM `{source_database_name}`.`{source_table_name}`
WHERE `{table_primary_key}`>={min_key_value}
AND `{table_primary_key}`<{max_key}
AND {data_condition} ;
""".format(target_database_name=self.target_database_name,
           source_database_name=self.source_database_name,
           target_table_name=self.target_table_name,
           source_table_name=self.source_table_name,
           min_key_value=pymysql.escape_string(str(min_key_value)),
           max_key_value=pymysql.escape_string(str(max_key_value)),
           data_condition=self.data_condition,
           table_primary_key=self.table_primary_key)
        delete_script = """
DELETE FROM `{target_database_name}`.`{target_table_name}`
WHERE `{table_primary_key}` IN (
SELECT `{table_primary_key}`
FROM `{source_database_name}`.`{source_table_name}`
WHERE `{table_primary_key}`>='{min_key_value}'
AND `{table_primary_key}`<='{max_key_value}')
AND `{table_primary_key}`>='{min_key_value}'
AND `{table_primary_key}`<='{max_key_value}'
AND {data_condition};
""".format(target_database_name=self.target_database_name,
           target_table_name=self.target_table_name,
           source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
           min_key_value=pymysql.escape_string(str(min_key_value)),
           max_key_value=pymysql.escape_string(str(max_key_value)),
           data_condition=self.data_condition,
           table_primary_key=self.table_primary_key)
        return {"insert_script": insert_script, "delete_script": delete_script}

    def loop_archive_data(self):
        if self.table_primary_key_type == "INT":
            max_key, min_key = self.get_key_range_int()
        else:
            max_key, min_key = self.get_key_range_char()
        if max_key is None or min_key is None:
            PrintHelper.print_info_message("未找到满足条件的键区间")
            return
        current_key = min_key
        while current_key <= max_key:
            if self.has_stop_file():
                break
            PrintHelper.print_info_message("*" * 70)
            if self.table_primary_key_type == "INT":
                next_key = self.get_next_key_int(min_key_value=current_key)
                transfer_scripts = self.get_archive_script_int(
                    min_key_value=min_key,
                    max_key_value=next_key)
            else:
                next_key = self.get_next_key_char(min_key_value=current_key)
                transfer_scripts = self.get_archive_script_char(
                    min_key_value=min_key,
                    max_key_value=next_key)
            self.archive_data_by_scripts(transfer_scripts)
            current_key = next_key
            PrintHelper.print_info_message("*" * 70)
            info = """最小值为：{0},最大值为：{1},当前处理值为：{2}""".format(
                min_key, max_key, current_key)
            PrintHelper.print_info_message(info)
        PrintHelper.print_info_message("*" * 70)
        PrintHelper.print_info_message("执行完成")

    def archive_data_by_scripts(self, transfer_scripts):
        sql_script_list = []
        insert_script = transfer_scripts["insert_script"]
        delete_script = transfer_scripts["delete_script"]
        temp_script = insert_script, None
        sql_script_list.append(temp_script)
        temp_script = delete_script, None
        sql_script_list.append(temp_script)
        sql_script = insert_script + delete_script
        tmp_script = """
USE {source_database_name};
BEGIN;
{sql_script}
COMMIT;
SELECT SLEEP('{batch_sleep_seconds}');
##=====================================================##
""".format(source_database_name=self.source_database_name,
           batch_sleep_seconds=self.batch_sleep_seconds,
           sql_script=sql_script)
        PrintHelper.write_file(file_path=self.transfer_script_file, message=tmp_script)
        if not self.is_dry_run:
            total_affect_rows = self.mysql_server.mysql_exec_many(sql_script_list)
            self.sleep_with_affect_rows(total_affect_rows)
        else:
            PrintHelper.print_info_message("生成迁移脚本(未执行)")
            PrintHelper.print_info_message(sql_script)

    def _user_confirm(self):
        info = """
将生成在服务器{mysql_host}上归档数据
迁移数据条件为:
INSERT INTO `{target_database_name}`.`{target_table_name}`
SELECT * FROM `{source_database_name}`.`{source_table_name}`
WHERE {data_condition}
"""
        info = info.format(mysql_host=self.mysql_server.mysql_host,
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

    def create_target_table(self):
        sql_script = "CREATE TABLE IF NOT EXISTS `{target_database_name}`.`{target_table_name}` LIKE `{source_database_name}`.`{source_table_name}`;".format(
            target_database_name=self.target_database_name,
            source_database_name=self.source_database_name,
            target_table_name=self.source_table_name,
            source_table_name=self.target_table_name)
        PrintHelper.print_info_message("建表脚本为：\n {0}".format(sql_script))
        self.mysql_server.mysql_exec(sql_script=sql_script)

    def check_config(self):
        try:
            self.create_target_table()
            if str(self.source_database_name).strip() == "":
                PrintHelper.print_warning_message("数据库名不能为空")
                return False
            if str(self.source_table_name).strip() == "":
                PrintHelper.print_warning_message("表名不能为空")
                return False
            if str(self.data_condition).strip() == "":
                PrintHelper.print_warning_message("迁移条件不能为空")
                return False
            source_columns = self.get_column_info_list(
                database_name=self.source_database_name,
                table_name=self.source_table_name)
            target_columns = self.get_column_info_list(
                database_name=self.target_database_name,
                table_name=self.target_table_name)
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
        if not self.check_config():
            return
        confirm_result = self._user_confirm()
        if confirm_result:
            self.loop_archive_data()
