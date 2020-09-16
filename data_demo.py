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
from dateutil.relativedelta import relativedelta
from mysql_archive import MyArchive
from print_helper import PrintHelper
from mysql_deleter import MyDeleter
from mysql_server import MySQLServer


def delete_demo():
    mysql_server = MySQLServer(
        mysql_host="172.20.117.60",
        mysql_port=3306,
        mysql_user="mysql_admin",
        mysql_password="mysql_admin_psw",
        mysql_charset="utf8mb4",
        database_name="testdb",
        connect_timeout=10
    )
    data_condition = "dt<'2020-08-20 11:22:30' and dt>='2020-08-20 10:57:10'"
    source_database_name = "testdb"
    source_table_name = "tb1001"
    mysql_deleter = MyDeleter(
        mysql_server=mysql_server,
        source_database_name=source_database_name,
        source_table_name=source_table_name,
        data_condition=data_condition,
        batch_scan_rows=1000,
        batch_sleep_seconds=1,
        max_query_seconds=100,
        is_dry_run=True,
        is_user_confirm=False
    )
    mysql_deleter.delete_data()
    mysql_deleter.is_dry_run = False
    mysql_deleter.is_user_confirm = True
    mysql_deleter.delete_data()


def archive_demo():
    mysql_server = MySQLServer(
        mysql_host="172.20.117.60",
        mysql_port=3306,
        mysql_user="mysql_admin",
        mysql_password="mysql_admin_psw",
        mysql_charset="utf8mb4",
        database_name="testdb",
        connect_timeout=10
    )
    source_database_name = "testdb"
    source_table_name = "tb1001"
    target_database_name = source_database_name + "_his"
    target_table_name = source_table_name
    data_condition = "dt<'2020-08-20 11:22:30' and dt>='2020-08-20 10:57:10'"
    mysql_archive = MyArchive(
        mysql_server=mysql_server,
        source_database_name=source_database_name,
        source_table_name=source_table_name,
        target_database_name=target_database_name,
        target_table_name=target_table_name,
        data_condition=data_condition,
        batch_scan_rows=1000,
        batch_sleep_seconds=1,
        max_query_seconds=100,
        is_dry_run=True,
        is_user_confirm=False
    )
    mysql_archive.set_loop_range(max_key=10000, min_key=0)
    mysql_archive.archive_data()
    mysql_archive.is_dry_run = False
    mysql_archive.is_user_confirm = True
    mysql_archive.archive_data()


def main():
    archive_demo()
    delete_demo()


if __name__ == '__main__':
    main()
