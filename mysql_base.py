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


class MyBase(object):
    def __init__(self, mysql_server,
                 source_database_name,
                 source_table_name,
                 data_condition,
                 batch_scan_rows,
                 batch_sleep_seconds,
                 is_dry_run=True,
                 is_user_confirm=True,
                 max_query_seconds=0):
        self.mysql_server = mysql_server
        self.source_database_name = source_database_name
        self.source_table_name = source_table_name
        self.data_condition = data_condition
        self.batch_scan_rows = batch_scan_rows
        self.batch_sleep_seconds = batch_sleep_seconds
        self.max_query_seconds = max_query_seconds
        self.is_dry_run = is_dry_run
        self.is_user_confirm = is_user_confirm
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

    def get_column_info_list(self, database_name, table_name):
        sql_script = """
DESC `{database_name}`.`{table_name}`
""".format(table_name=table_name, database_name=database_name)
        query_result = self.mysql_server.mysql_query(sql_script=sql_script, return_dict=True)
        return query_result

    def get_key_range_int(self):
        """
        按照传入的表获取要删除数据最大ID、最小ID、删除总行数
        :return: 返回要删除数据最大ID、最小ID、删除总行数
        """
        if (self.min_key is not None) and (self.max_key is not None):
            return self.max_key, self.min_key
        sql_script = """
SELECT
IFNULL(MAX(`{table_primary_key}`),0) AS max_key,
IFNULL(MIN(`{table_primary_key}`),0) AS min_key
FROM `{source_database_name}`.`{source_table_name}`
WHERE {delete_condition};
""".format(source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
           delete_condition=self.data_condition,
           table_primary_key=self.table_primary_key)
        query_result = self.mysql_server.mysql_query(
            sql_script=sql_script,
            max_execute_seconds=self.max_query_seconds,
            return_dict=True
        )
        if len(query_result) == 0:
            return None, None
        else:
            return query_result[0]["max_key"], query_result[0]["min_key"]

    def get_key_range_char(self):
        """
        按照传入的表获取要删除数据最大ID、最小ID、删除总行数
        :return: 返回要删除数据最大ID、最小ID、删除总行数
        """
        if (self.min_key is not None) and (self.max_key is not None):
            return self.max_key, self.min_key
        sql_script = """
SELECT
IFNULL(MAX(`{table_primary_key}`),'') AS max_key,
IFNULL(MIN(`{table_primary_key}`),'') AS min_key
FROM `{source_database_name}`.`{source_table_name}`
""".format(source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
           table_primary_key=self.table_primary_key)
        query_result = self.mysql_server.mysql_query(
            sql_script=sql_script,
            max_execute_seconds=self.max_query_seconds,
            return_dict=True
        )
        if len(query_result) == 0:
            return None, None
        else:
            return str(query_result[0]["max_key"].encode('utf8')), str(query_result[0]["min_key"].encode('utf8'))

    def get_next_key_char(self, min_key_value):
        sql_script = """
SELECT MAX(`{table_primary_key}`) AS max_key_value FROM (
SELECT `{table_primary_key}`
FROM `{source_database_name}`.`{source_table_name}`
WHERE {table_primary_key} >'{min_key_value}'
ORDER BY `{table_primary_key}` ASC
LIMIT {batch_scan_rows}
) AS T1;
""".format(source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
           table_primary_key=self.table_primary_key,
           min_key_value=pymysql.escape_string(str(min_key_value)),
           batch_scan_rows=self.batch_scan_rows)
        query_result = self.mysql_server.mysql_query(sql_script=sql_script, sql_paras=None, return_dict=True)
        if len(query_result) > 0:
            return str(query_result[0]["max_key_value"].encode('utf8'))
        else:
            return None

    def get_next_key_int(self, min_key_value):
        sql_script = """
SELECT MAX(`{table_primary_key}`) AS max_key_value FROM (
SELECT `{table_primary_key}`
FROM `{source_database_name}`.`{source_table_name}`
WHERE {table_primary_key} >'{min_key_value}'
ORDER BY `{table_primary_key}` ASC
LIMIT {batch_scan_rows}
) AS T1;
""".format(source_database_name=self.source_database_name,
           source_table_name=self.source_table_name,
           table_primary_key=self.table_primary_key,
           min_key_value=min_key_value,
           batch_scan_rows=self.batch_scan_rows)
        query_result = self.mysql_server.mysql_query(sql_script=sql_script, sql_paras=None, return_dict=True)
        if len(query_result) > 0:
            return query_result[0]["max_key_value"]
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
        self.max_key = max_key
        self.min_key = min_key
