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
from mysql_remote_archive import MyRemoteArchive
from mysql_local_archive import MyLocalArchive
from print_helper import PrintHelper
from mysql_deleter import MyDeleter
from mysql_server import MySQLServer


def delete_demo():
    source_mysql_server = MySQLServer(
        mysql_host="192.168.0.1",
        mysql_port=3306,
        mysql_user="mysql_test_admin",
        mysql_password="mysql_test_admin.com",
        mysql_charset="utf8mb4",
        database_name="testdb",
        connect_timeout=10
    )
    data_condition = "dt<'2020-01-26 21:49:35' and dt>='2020-08-20 10:57:10'"
    source_table_name = "tb1001"
    mysql_deleter = MyDeleter(
        source_mysql_server=source_mysql_server,
        source_table_name=source_table_name,
        data_condition=data_condition,
        batch_scan_rows=10000,
        batch_sleep_seconds=1,
        max_query_seconds=100,
        is_dry_run=True,
        is_user_confirm=False,
        force_table_scan=True
    )
    mysql_deleter.delete_data()
    mysql_deleter.is_dry_run = False
    mysql_deleter.is_user_confirm = True
    mysql_deleter.delete_data()


def archive_local_demo():
    source_mysql_server = MySQLServer(
        mysql_host="192.168.0.1",
        mysql_port=3306,
        mysql_user="mysql_test_admin",
        mysql_password="mysql_test_admin.com",
        mysql_charset="utf8mb4",
        database_name="testdb",
        connect_timeout=10
    )
    source_table_name = "tb1001"
    target_database_name = source_mysql_server.database_name + "_his"
    target_table_name = source_table_name + "_his"
    data_condition = "dt<'2020-08-20 13:22:30' and dt>='2020-08-20 10:57:10'"
    mysql_archive = MyLocalArchive(
        source_mysql_server=source_mysql_server,
        source_table_name=source_table_name,
        target_database_name=target_database_name,
        target_table_name=target_table_name,
        data_condition=data_condition,
        batch_scan_rows=10000,
        batch_sleep_seconds=1,
        max_query_seconds=100,
        is_dry_run=True,
        is_user_confirm=False
    )
    mysql_archive.archive_data()
    mysql_archive.is_dry_run = False
    mysql_archive.is_user_confirm = True
    mysql_archive.archive_data()


def archive_remote_demo():
    source_mysql_server = MySQLServer(
        mysql_host="192.168.0.1",
        mysql_port=3306,
        mysql_user="mysql_test_admin",
        mysql_password="mysql_test_admin.com",
        mysql_charset="utf8mb4",
        database_name="testdb",
        connect_timeout=10
    )
    target_mysql_server = MySQLServer(
        mysql_host="192.168.0.1",
        mysql_port=3306,
        mysql_user="mysql_test_admin",
        mysql_password="mysql_test_admin.com",
        mysql_charset="utf8mb4",
        database_name="testdb_his",
        connect_timeout=10
    )
    source_table_name = "tb1001"
    target_table_name = source_table_name + "_his"
    data_condition = "dt<'2020-08-20 13:22:30' and dt>='2020-08-20 10:57:10'"
    mysql_archive = MyRemoteArchive(
        source_mysql_server=source_mysql_server,
        source_table_name=source_table_name,
        target_mysql_server=target_mysql_server,
        target_table_name=target_table_name,
        data_condition=data_condition,
        batch_scan_rows=10000,
        batch_sleep_seconds=1,
        max_query_seconds=100,
        is_dry_run=True,
        is_user_confirm=False
    )
    mysql_archive.archive_data()
    mysql_archive.is_dry_run = False
    mysql_archive.is_user_confirm = True
    mysql_archive.archive_data()


def main():
    # archive_demo()
    delete_demo()


if __name__ == '__main__':
    main()
