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
import datetime
import time
import pymysql
import traceback
from print_helper import PrintHelper


class MySQLServer(object):
    def __init__(self, mysql_host,
                 mysql_user,
                 mysql_password,
                 mysql_port,
                 database_name,
                 mysql_charset,
                 connect_timeout):
        self.mysql_host = mysql_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_port = mysql_port
        self.connect_timeout = connect_timeout
        self.mysql_charset = mysql_charset
        self.database_name = database_name

    def get_connection(self, return_dict=False, connect_timeout=None):
        """
        获取当前服务器的MySQL连接
        :return:
        """
        if connect_timeout is None:
            connect_timeout = self.connect_timeout
        if return_dict:
            conn = pymysql.connect(
                host=self.mysql_host,
                user=self.mysql_user,
                passwd=self.mysql_password,
                port=self.mysql_port,
                connect_timeout=connect_timeout,
                charset=self.mysql_charset,
                db=self.database_name,
                cursorclass=pymysql.cursors.DictCursor
            )
        else:
            conn = pymysql.connect(
                host=self.mysql_host,
                user=self.mysql_user,
                passwd=self.mysql_password,
                port=self.mysql_port,
                connect_timeout=connect_timeout,
                charset=self.mysql_charset,
                db=self.database_name,
                cursorclass=pymysql.cursors.Cursor
            )
        return conn

    def mysql_query(self, sql_script, sql_paras=None, return_dict=False, max_execute_seconds=0):
        conn = None
        cursor = None
        try:
            conn = self.get_connection(return_dict=return_dict)
            cursor = conn.cursor()
            if max_execute_seconds != 0:
                try:
                    cursor.execute("set max_statement_time={};".format(max_execute_seconds * 1000))
                except:
                    pass
                try:
                    cursor.execute("set max_execution_time={}".format(max_execute_seconds * 1000))
                except:
                    pass
            if sql_paras is not None:
                cursor.execute(sql_script, sql_paras)
            else:
                cursor.execute(sql_script)
            exec_result = cursor.fetchall()
            conn.commit()
            return exec_result
        except Exception as ex:
            warning_message = """
        execute script:{mysql_script}
        execute paras:{mysql_paras},
        execute exception:{mysql_exception}
        execute traceback:{mysql_traceback}
        """.format(
                mysql_script=sql_script,
                mysql_paras=str(sql_paras),
                mysql_exception=str(ex),
                mysql_traceback=traceback.format_exc()
            )
            PrintHelper.print_warning_message(warning_message)
            raise Exception(str(ex))
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    def mysql_exec(self, sql_script, sql_paras=None):
        """
        执行传入的脚本，返回影响行数
        :param sql_script:
        :param sql_param:
        :return: 脚本最后一条语句执行影响行数
        """
        cursor = None
        conn = None
        try:
            conn = self.get_connection()
            info_message = "在服务器{0}上执行脚本:{1}".format(
                conn.get_host_info(),
                sql_script)
            PrintHelper.print_info_message(info_message)
            cursor = conn.cursor()
            if sql_paras is not None:
                cursor.execute(sql_script, sql_paras)
            else:
                cursor.execute(sql_script)
            affect_rows = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            return affect_rows
        except Exception as ex:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.rollback()
                conn.close()
            warning_message = "Exception in mysql_query:{0},{1}".format(
                str(ex), traceback.format_exc())
            PrintHelper.print_warning_message(warning_message)
            raise Exception(str(ex))

    def mysql_exec_many(self, script_list):
        """
        将多个SQL放到一个事务中执行
        :param script_list:
        :return:
        """
        cursor = None
        conn = None
        total_affect_rows = 0
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            for sql_script, sql_param in script_list:
                info_message = "在服务器{0}上执行脚本:{1},\r参数：{2}".format(
                    conn.get_host_info(),
                    sql_script, str(sql_param))
                PrintHelper.print_info_message(info_message)
                if sql_param is not None:
                    cursor.execute(sql_script, sql_param)
                else:
                    cursor.execute(sql_script)
                affect_rows = cursor.rowcount
                PrintHelper.print_info_message("影响行数：{0}".format(affect_rows))
                if affect_rows is not None:
                    total_affect_rows += affect_rows
            conn.commit()
            return total_affect_rows
        except Exception as ex:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.rollback()
                conn.close()
            warning_message = "Exception in mysql_query:{0},{1}".format(
                str(ex), traceback.format_exc())
            PrintHelper.print_warning_message(warning_message)
            raise Exception(str(ex))
