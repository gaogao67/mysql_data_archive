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


class MyDeleter(MyBase):
    def __init__(self, mysql_server,
                 source_database_name,
                 source_table_name,
                 data_condition,
                 batch_scan_rows,
                 batch_sleep_seconds,
                 is_dry_run=True,
                 is_user_confirm=True,
                 max_query_seconds=0):
        super(MyDeleter, self).__init__(
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

    def get_delete_script_int(self, min_key_value, max_key_value):
        delete_script = """
DELETE FROM `{source_database_name}`.`{source_table_name}`
WHERE `{table_primary_key}`>={min_key_value}
AND `{table_primary_key}`<{max_key_value}
AND {data_condition};
""".format(
            source_database_name=self.source_database_name,
            source_table_name=self.source_table_name,
            min_key_value=min_key_value,
            max_key_value=max_key_value,
            data_condition=self.data_condition,
            table_primary_key=self.table_primary_key
        )
        return {"delete_script": delete_script}

    def get_delete_script_char(self, min_key_value, max_key_value):
        delete_script = """
DELETE FROM `{source_database_name}`.`{source_table_name}`
WHERE `{table_primary_key}`>='{min_key_value}'
AND `{table_primary_key}`<'{max_key_value}'
AND {data_condition};
""".format(
            source_database_name=self.source_database_name,
            source_table_name=self.source_table_name,
            min_key_value=pymysql.escape_string(str(min_key_value)),
            max_key_value=pymysql.escape_string(str(max_key_value)),
            data_condition=self.data_condition,
            table_primary_key=self.table_primary_key
        )
        return {"delete_script": delete_script}

    def loop_delete_data(self):
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
                transfer_scripts = self.get_delete_script_int(
                    min_key_value=min_key,
                    max_key_value=next_key)
            else:
                next_key = self.get_next_key_char(min_key_value=current_key)
                transfer_scripts = self.get_delete_script_char(
                    min_key_value=min_key,
                    max_key_value=next_key)
            self.delete_data_by_scripts(transfer_scripts)
            current_key = next_key
            PrintHelper.print_info_message("*" * 70)
            info = """最小值为：{0},最大值为：{1},当前处理值为：{2}""".format(
                min_key, max_key, current_key)
            PrintHelper.print_info_message(info)
        PrintHelper.print_info_message("*" * 70)
        PrintHelper.print_info_message("执行完成")

    def delete_data_by_scripts(self, transfer_scripts):
        sql_script_list = []
        delete_script = transfer_scripts["delete_script"]

        temp_script = delete_script, None
        sql_script_list.append(temp_script)
        sql_script = delete_script
        tmp_script = """
USE {0};
""".format(self.source_database_name) + sql_script + """
COMMIT;
SELECT SLEEP('{0}');
##=====================================================##
""".format(self.batch_sleep_seconds)
        PrintHelper.write_file(file_path=self.transfer_script_file, message=tmp_script)
        if not self.is_dry_run:
            total_affect_rows = self.mysql_server.mysql_exec_many(sql_script_list)
            self.sleep_with_affect_rows(total_affect_rows)
        else:
            PrintHelper.print_info_message("生成迁移脚本(未执行)")
            PrintHelper.print_info_message(sql_script)

    def user_confirm(self):
        info = """
将生成在服务器{mysql_host}上{source_database_name}中删除表{source_table_name}中数据。
删除数据条件为:
DELETE FROM `{source_database_name}`.`{source_table_name}`
WHERE {data_condition}
""".format(mysql_host=self.mysql_server.mysql_host,
           source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
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
            source_columns = self.get_column_info_list(
                database_name=self.source_database_name,
                table_name=self.source_table_name)
            primary_key_count = 0
            for column_item in source_columns:
                source_column_name = column_item["Field"]
                source_column_key = column_item["Key"]
                source_column_type = column_item["Type"]
                if str(source_column_key).lower() == 'pri':
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

    def delete_data(self):
        if not self.check_config():
            return
        confirm_result = self.user_confirm()
        if confirm_result:
            self.loop_delete_data()
