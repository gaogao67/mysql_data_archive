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
from mysql_server import MySQLServer


class MyLocalArchive(MyBase):
    def __init__(self, source_mysql_server,
                 source_table_name,
                 target_database_name,
                 target_table_name,
                 data_condition,
                 batch_scan_rows,
                 batch_sleep_seconds,
                 is_dry_run=True,
                 is_user_confirm=True,
                 max_query_seconds=0,
                 force_table_scan=False):
        target_mysql_server = MySQLServer(
            mysql_host=source_mysql_server.mysql_host,
            mysql_port=source_mysql_server.mysql_port,
            mysql_user=source_mysql_server.mysql_user,
            mysql_password=source_mysql_server.mysql_password,
            mysql_charset=source_mysql_server.mysql_charset,
            database_name=target_database_name,
            connect_timeout=source_mysql_server.connect_timeout
        )
        super(MyLocalArchive, self).__init__(
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

    def get_archive_script(self, current_key, next_key):
        insert_script = """
INSERT INTO `{target_database_name}`.`{target_table_name}`
SELECT * FROM `{source_database_name}`.`{source_table_name}`
WHERE `{table_primary_key}`>='{current_key}'
AND `{table_primary_key}`<='{next_key}'
AND {data_condition} ;
""".format(target_database_name=self.target_database_name,
           source_database_name=self.source_database_name,
           target_table_name=self.target_table_name,
           source_table_name=self.source_table_name,
           current_key=pymysql.escape_string(str(current_key)),
           next_key=pymysql.escape_string(str(next_key)),
           data_condition=self.data_condition,
           table_primary_key=self.table_primary_key)
        delete_script = """
DELETE FROM `{source_database_name}`.`{source_table_name}`
WHERE `{table_primary_key}` IN (
SELECT `{table_primary_key}`
FROM `{target_database_name}`.`{target_table_name}`
WHERE `{table_primary_key}`>='{current_key}'
AND `{table_primary_key}`<='{next_key}')
AND `{table_primary_key}`>='{current_key}'
AND `{table_primary_key}`<='{next_key}'
AND {data_condition};
""".format(target_database_name=self.target_database_name,
           target_table_name=self.target_table_name,
           source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
           current_key=pymysql.escape_string(str(current_key)),
           next_key=pymysql.escape_string(str(next_key)),
           data_condition=self.data_condition,
           table_primary_key=self.table_primary_key)
        return {"insert_script": insert_script, "delete_script": delete_script}

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
            info = """最小值为：{0},最大值为：{1},当前处理值为：{2}""".format(
                min_key, max_key, current_key)
            PrintHelper.print_info_message(info)
            next_key = self.get_next_loop_key(current_key=current_key)
            if next_key is None:
                PrintHelper.print_info_message("未找到下一个可归档的区间，退出归档")
                break
            transfer_scripts = self.get_archive_script(
                current_key=current_key,
                next_key=next_key)
            if not self.archive_data_by_scripts(transfer_scripts):
                PrintHelper.print_info_message("执行出现异常，退出归档！")
                break
            current_key = next_key
        PrintHelper.print_info_message("*" * 70)
        PrintHelper.print_info_message("执行完成")

    def archive_data_by_scripts(self, transfer_scripts):
        try:
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
                total_affect_rows = self.source_mysql_server.mysql_exec_many(sql_script_list)
                PrintHelper.print_info_message("本次归档操作记录{}条".format(int(total_affect_rows / 2)))
                self.sleep_with_affect_rows(total_affect_rows)
            else:
                PrintHelper.print_info_message("生成迁移脚本(未执行)")
                PrintHelper.print_info_message(sql_script)
            return True
        except Exception as ex:
            PrintHelper.print_warning_message("在归档过程中出现异常：{}\n堆栈：{}\n".format(str(ex), traceback.format_exc()))
            return False

    def _user_confirm(self):
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

    def create_target_table(self):
        sql_script = "CREATE TABLE IF NOT EXISTS `{target_database_name}`.`{target_table_name}` LIKE `{source_database_name}`.`{source_table_name}`;".format(
            target_database_name=self.target_database_name,
            source_database_name=self.source_database_name,
            target_table_name=self.target_table_name,
            source_table_name=self.source_table_name)
        PrintHelper.print_info_message("建表脚本为：\n {0}".format(sql_script))
        self.source_mysql_server.mysql_exec(sql_script=sql_script)

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
            if self.target_mysql_server.mysql_host != self.source_mysql_server.mysql_host and self.target_mysql_server.mysql_port != self.source_mysql_server.mysql_port:
                PrintHelper.print_warning_message("源服务器和目标服务器不同")
                return False
            if self.target_database_name == self.source_database_name and self.target_table_name == self.source_table_name:
                PrintHelper.print_warning_message("源表和目标表相同")
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
        if not self.check_config():
            return
        confirm_result = self._user_confirm()
        if confirm_result:
            self.loop_archive_data()
