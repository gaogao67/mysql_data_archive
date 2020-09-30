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
from mysql_server import MySQLServer


class MyBase(object):
    def __init__(self, source_mysql_server: MySQLServer,
                 source_table_name: str,
                 data_condition: str,
                 batch_scan_rows: int,
                 batch_sleep_seconds,
                 is_dry_run=True,
                 is_user_confirm=True,
                 max_query_seconds=0,
                 target_mysql_server=None,
                 target_table_name=None,
                 force_table_scan=False):
        self.source_mysql_server = source_mysql_server
        self.source_database_name = self.source_mysql_server.database_name
        self.source_table_name = source_table_name
        self.target_mysql_server = target_mysql_server
        if self.target_mysql_server is None:
            self.target_database_name = None
        else:
            self.target_database_name = self.target_mysql_server.database_name
        self.target_table_name = target_table_name
        self.data_condition = data_condition
        self.batch_scan_rows = batch_scan_rows
        self.batch_sleep_seconds = batch_sleep_seconds
        self.max_query_seconds = max_query_seconds
        self.is_dry_run = is_dry_run
        self.is_user_confirm = is_user_confirm
        self.force_table_scan = force_table_scan
        self.working_folder = os.path.dirname(os.path.abspath(__file__))
        self.script_folder = os.path.join(self.working_folder, "scripts")
        if not os.path.exists(self.script_folder):
            os.makedirs(self.script_folder)
        self.transfer_script_file = os.path.join(
            self.script_folder,
            "transfer_script_{}.txt".format(datetime.datetime.now().strftime("%Y%m%d%H%M%SZ")))
        self.stop_script_file = os.path.join(self.script_folder, "stop.txt")
        self.table_primary_key = ""
        self.table_primary_key_type = ""
        self.max_key = None
        self.min_key = None

    def get_source_columns(self):
        return self.get_column_info_list(
            mysql_server=self.source_mysql_server,
            database_name=self.source_database_name,
            table_name=self.source_table_name
        )

    def get_target_columns(self):
        return self.get_column_info_list(
            mysql_server=self.target_mysql_server,
            database_name=self.target_database_name,
            table_name=self.target_table_name
        )

    def get_column_info_list(self, mysql_server: MySQLServer, database_name, table_name):
        """
        获取指定表的列信息
        :param mysql_server:
        :param database_name:
        :param table_name:
        :return:
        """
        sql_script = """
DESC `{database_name}`.`{table_name}`
""".format(table_name=table_name, database_name=database_name)
        query_result = mysql_server.mysql_query(sql_script=sql_script, return_dict=True)
        return query_result

    def get_loop_key_range(self):
        """
        按照传入的表获取要删除数据最大ID、最小ID、删除总行数
        :return: 返回要删除数据最大ID、最小ID、删除总行数
        """
        if (self.min_key is not None) and (self.max_key is not None):
            return self.max_key, self.min_key
        if self.force_table_scan:
            sql_script = """
SELECT
MAX(`{table_primary_key}`) AS max_key,
MIN(`{table_primary_key}`) AS min_key
FROM `{source_database_name}`.`{source_table_name}`;
""".format(source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
           table_primary_key=self.table_primary_key)
        else:
            sql_script = """
SELECT
MAX(`{table_primary_key}`) AS max_key,
MIN(`{table_primary_key}`) AS min_key
FROM `{source_database_name}`.`{source_table_name}`
WHERE {data_condition};
""".format(source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
           table_primary_key=self.table_primary_key,
           data_condition=self.data_condition)
        query_result = self.source_mysql_server.mysql_query(
            sql_script=sql_script,
            max_execute_seconds=self.max_query_seconds,
            return_dict=True
        )
        if query_result[0]["max_key"] is None or query_result[0]["min_key"] is None:
            return None, None
        if self.table_primary_key_type == "INT":
            return query_result[0]["max_key"], query_result[0]["min_key"]
        else:
            return str(query_result[0]["max_key"].encode('utf8')), str(query_result[0]["min_key"].encode('utf8'))

    def get_next_loop_key(self, current_key):
        sql_script = """
SELECT MAX(`{table_primary_key}`) AS max_key_value FROM (
SELECT `{table_primary_key}`
FROM `{source_database_name}`.`{source_table_name}`
WHERE {table_primary_key} >='{current_key}'
ORDER BY `{table_primary_key}` ASC
LIMIT {batch_scan_rows}
) AS T1;
""".format(source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
           table_primary_key=self.table_primary_key,
           current_key=pymysql.escape_string(str(current_key)),
           batch_scan_rows=self.batch_scan_rows)
        query_result = self.source_mysql_server.mysql_query(sql_script=sql_script, sql_paras=None, return_dict=True)
        if len(query_result) > 0:
            if self.table_primary_key_type == "INT":
                return query_result[0]["max_key_value"]
            else:
                return str(query_result[0]["max_key_value"].encode('utf8'))
        else:
            return None

    def has_stop_file(self):
        if os.path.exists(self.stop_script_file):
            PrintHelper.print_info_message("检查到停止文件，准备退出！")
            return True
        else:
            return False

    def get_user_choose_option(self, input_options, input_message):
        while True:
            PrintHelper.print_info_message(input_message)
            str_input = input("")
            for input_option in input_options:
                if str_input.strip() == input_option:
                    choose_option = input_option
                    return choose_option

    def sleep_with_affect_rows(self, total_affect_rows):
        time.sleep(round(self.batch_sleep_seconds * total_affect_rows / self.batch_scan_rows, 1))

    def set_loop_range(self, max_key, min_key):
        """
        设置循环键的最大值和最小值
        :param max_key:
        :param min_key:
        :return:
        """
        self.max_key = max_key
        self.min_key = min_key

    def is_target_table_exist(self):
        """
        判断目标是否存在
        :return:
        """
        sql_script = """
SELECT * 
FROM information_schema.tables
WHERE TABLE_SCHEMA=%s
AND TABLE_NAME=%s;
"""
        query_result = self.target_mysql_server.mysql_query(
            sql_script=sql_script,
            sql_paras=[self.target_database_name, self.target_table_name],
            return_dict=False
        )
        if len(query_result) > 0:
            return True
        else:
            return False
