# coding: utf-8
##===============================================================##
## 用于批量清理表中无用数据，要求表上有唯一主键。
## 通过循环主键来清理数据，限制单个事务操作数据量。
##===============================================================##


## import pymysql
## pymysql.install_as_MySQLdb()
import MySQLdb
import time, datetime
import os, traceback
from dateutil.relativedelta import relativedelta


class PrintHelper(object):
    working_folder = os.path.dirname(os.path.abspath(__file__))
    default_log_file = os.path.join(working_folder, "default_log.txt")
    error_log_file = os.path.join(working_folder, "error_log.txt")

    @staticmethod
    def highlight(s):
        return "%s[30;2m%s%s[1m" % (chr(27), s, chr(27))

    @staticmethod
    def write_file(file_path, message):
        """
        将传入的message追加写入到file_path指定的文件中
        请先创建文件所在的目录
        :param file_path: 要写入的文件路径
        :param message: 要写入的信息
        :return:
        """
        file_handle = open(file_path, 'a')
        file_handle.writelines(message)
        # 追加一个换行以方便浏览
        file_handle.writelines(chr(10))
        file_handle.flush()
        file_handle.close()

    @staticmethod
    def print_warning_message(message):
        """
        以红色字体显示消息内容
        :param message: 消息内容
        :return: 无返回值
        """
        message = str(message)
        print(PrintHelper.highlight('') + "%s[31;1m%s%s[0m" % (chr(27), message, chr(27)))
        PrintHelper.write_file(PrintHelper.error_log_file, message)

    @staticmethod
    def print_info_message(message):
        """
        以绿色字体输出提醒性的消息
        :param message: 消息内容
        :return: 无返回值
        """
        message = str(message)
        print(PrintHelper.highlight('') + "%s[32;2m%s%s[0m" % (chr(27), message, chr(27)))
        PrintHelper.write_file(PrintHelper.default_log_file, message)


class MysqlServer(object):
    def __init__(self, mysql_host, mysql_port, database_name, mysql_user,
                 mysql_password, mysql_charset="utf8", connection_timeout=60):
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_charset = mysql_charset
        self.connection_timeout = connection_timeout
        self.database_name = database_name

    def get_connection(self):
        return MySQLdb.connect(
            host=self.mysql_host,
            port=self.mysql_port,
            user=self.mysql_user,
            passwd=self.mysql_password,
            connect_timeout=self.connection_timeout,
            charset=self.mysql_charset,
            db=self.database_name)

    def mysql_exec(self, sql_script, sql_param=None):
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
            if sql_param is not None:
                cursor.execute(sql_script, sql_param)
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

    def mysql_query(self, sql_script, sql_param=None):
        """
        执行传入的SQL脚本，并返回查询结果
        :param sql_script:
        :param sql_param:
        :return: 返回SQL查询结果
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
            if sql_param is not None:
                cursor.execute(sql_script, sql_param)
            else:
                cursor.execute(sql_script)
            exec_result = cursor.fetchall()
            cursor.close()
            conn.close()
            return exec_result
        except Exception as ex:
            if cursor is not None:
                cursor.close()
            if conn is not None:
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
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            exec_result_list = []
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
                exec_result_list.append(affect_rows)
            conn.commit()
            return exec_result_list
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


class MyDeleter(object):
    def __init__(self, mysql_server, database_name,
                 source_table_name,
                 transfer_condition, transfer_batch_rows,
                 batch_sleep_seconds,
                 is_dry_run=True, is_user_confirm=True):
        self.mysql_server = mysql_server
        self.database_name = database_name
        self.source_table_name = source_table_name
        self.transfer_condition = transfer_condition
        self.transfer_batch_rows = transfer_batch_rows
        self.table_primary_key = ""
        self.table_primary_key_type = ""
        self.batch_sleep_seconds = batch_sleep_seconds
        self.is_dry_run = is_dry_run
        self.is_user_confirm = is_user_confirm
        working_folder = os.path.dirname(os.path.abspath(__file__))
        self.transfer_script_file = os.path.join(working_folder, "transfer_script.txt")

    def get_column_info_list(self, table_name):
        sql_script = """
    DESC {0}
    """.format(table_name)
        column_info_list = []
        query_result = self.mysql_server.mysql_query(sql_script=sql_script, sql_param=None)
        for row in query_result:
            column_name = row[0]
            column_type = row[1]
            column_key = row[3]
            column_info = column_name, column_key, column_type
            column_info_list.append(column_info)
        return column_info_list

    def get_id_range(self):
        """
        按照传入的表获取要删除数据最大ID、最小ID、删除总行数
        :return: 返回要删除数据最大ID、最小ID、删除总行数
        """
        sql_script = """
SELECT
IFNULL(MAX({2}),0) AS MAX_ID,
IFNULL(MIN({2}),0) AS MIN_ID,
COUNT(1) AS Total_Count
FROM {0}
WHERE {1};
""".format(self.source_table_name, self.transfer_condition, self.table_primary_key)

        query_result = self.mysql_server.mysql_query(sql_script=sql_script, sql_param=None)
        max_id, min_id, total_count = query_result[0]
        # 此处有一坑，可能出现total_count不为0 但是max_id 和min_id 为None的情况
        # 因此判断max_id和min_id 是否为NULL
        if (max_id is None) or (min_id is None):
            max_id, min_id, total_count = 0, 0, 0
        return max_id, min_id, total_count

    def get_char_range(self):
        """
        按照传入的表获取要删除数据最大ID、最小ID、删除总行数
        :return: 返回要删除数据最大ID、最小ID、删除总行数
        """
        sql_script = """
SELECT
IFNULL(MAX({table_primary_key}),'') AS max_id,
IFNULL(MIN({table_primary_key}),'') AS min_id
FROM {source_table_name}
""".format(source_table_name=self.source_table_name, table_primary_key=self.table_primary_key)
        query_result = self.mysql_server.mysql_query(sql_script=sql_script, sql_param=None)
        if len(query_result) > 0:
            max_key, min_key = str(query_result[0][0].encode('utf8')), str(
                query_result[0][1].encode('utf8'))
        else:
            max_key, min_key = "", ""
        if (max_key is None) or (min_key is None):
            max_key, min_key = "", ""
        return max_key, min_key

    def get_transfer_script_int(self, current_min_id, current_max_id):
        delete_script = """
DELETE FROM {source_table_name}
WHERE {table_primary_key}>={current_min_id}
AND {table_primary_key}<{current_max_id}
AND {transfer_condition};
""".format(
            source_table_name=self.source_table_name,
            current_min_id=current_min_id,
            current_max_id=current_max_id,
            transfer_condition=self.transfer_condition,
            table_primary_key=self.table_primary_key
        )
        return {"delete_script": delete_script}

    def get_transfer_script_char(self, current_min_id, current_max_id):
        delete_script = """
DELETE FROM {source_table_name}
WHERE `{table_primary_key}`>='{current_min_id}'
AND `{table_primary_key}`<'{current_max_id}'
AND {transfer_condition};
""".format(
            source_table_name=self.source_table_name,
            current_min_id=MySQLdb.escape_string(str(current_min_id)),
            current_max_id=MySQLdb.escape_string(str(current_max_id)),
            transfer_condition=self.transfer_condition,
            table_primary_key=self.table_primary_key
        )
        return {"delete_script": delete_script}

    def get_next_char_key(self, min_key):
        sql_script = """
SELECT MAX(`{table_primary_key}`) FROM (
SELECT `{table_primary_key}`
FROM `{source_table_name}`
WHERE {table_primary_key} >'{min_key}'
ORDER BY `{table_primary_key}` ASC
LIMIT {transfer_batch_rows}
) AS T1;
""".format(source_table_name=self.source_table_name, table_primary_key=self.table_primary_key,
           min_key=MySQLdb.escape_string(str(min_key)), transfer_batch_rows=self.transfer_batch_rows)
        query_result = self.mysql_server.mysql_query(sql_script=sql_script, sql_param=None)
        if len(query_result) > 0:
            temp_key = query_result[0][0]
            if temp_key is None:
                next_key = None
            else:
                next_key = str(temp_key.encode('utf8'))
        else:
            next_key = None
        return next_key

    def loop_transfer_data_char(self):
        max_key, min_key = self.get_char_range()
        current_key = ""
        next_key = self.get_next_char_key(current_key)
        while next_key is not None:
            PrintHelper.print_info_message("*" * 70)
            transfer_scripts = self.get_transfer_script_char(
                current_min_id=min_key,
                current_max_id=next_key)
            self.transfer_data_by_scripts(transfer_scripts)
            current_key = next_key
            PrintHelper.print_info_message("*" * 70)
            next_key = self.get_next_char_key(current_key)
            info = """
最小值为：{0}
最大值为：{1}
当前处理值为：{2}
""".format(min_key, max_key, current_key)
            PrintHelper.print_info_message(info)
        PrintHelper.print_info_message("*" * 70)
        PrintHelper.print_info_message("执行完成")

    def loop_transfer_data_int(self):
        max_id, min_id, total_count = self.get_id_range()
        if min_id == 0:
            PrintHelper.print_info_message("无数据需要结转")
            return
        current_min_id = min_id
        while current_min_id <= max_id:
            PrintHelper.print_info_message("*" * 70)
            current_max_id = current_min_id + self.transfer_batch_rows
            transfer_scripts = self.get_transfer_script_int(
                current_min_id,
                current_max_id)
            self.transfer_data_by_scripts(transfer_scripts)
            current_percent = (current_max_id - min_id) * 100.0 / (max_id - min_id)
            left_rows = max_id - current_max_id
            if left_rows < 0:
                left_rows = 0
            current_percent_str = "%.2f" % current_percent
            info = "当前进度{0}/{1},剩余{2},进度为{3}%"
            info = info.format(current_max_id, max_id, left_rows,
                               current_percent_str)
            PrintHelper.print_info_message(info)
            current_min_id = current_max_id
        PrintHelper.print_info_message("*" * 70)
        PrintHelper.print_info_message("执行完成")

    def transfer_data_by_scripts(self, transfer_scripts):
        sql_script_list = []
        delete_script = transfer_scripts["delete_script"]

        temp_script = delete_script, None
        sql_script_list.append(temp_script)
        sql_script = delete_script
        tmp_script = """
USE {0};
""".format(self.database_name) + sql_script + """
COMMIT;
SELECT SLEEP('{0}');
##=====================================================##
""".format(self.batch_sleep_seconds)
        PrintHelper.write_file(file_path=self.transfer_script_file, message=tmp_script)
        if not self.is_dry_run:
            exec_result_list = self.mysql_server.mysql_exec_many(sql_script_list)
            PrintHelper.print_info_message("执行结果:")
            for item in exec_result_list:
                PrintHelper.print_info_message("影响行数：" + str(item))
            time.sleep(self.batch_sleep_seconds)
        else:
            PrintHelper.print_info_message("生成迁移脚本(未执行)")
            PrintHelper.print_info_message(sql_script)

    def get_user_choose_option(self, input_options, input_message):
        while_flag = True
        choose_option = None
        while while_flag:
            PrintHelper.print_info_message(input_message)
            str_input = raw_input("")
            for input_option in input_options:
                if str_input.strip() == input_option:
                    choose_option = input_option
                    while_flag = False
        return choose_option

    def user_confirm(self):
        info = """
将生成在服务器{mysql_host}上{database_name}中删除表{source_table_name}中数据。
删除数据条件为:
DELETE FROM `{source_table_name}`
WHERE {transfer_condition}
"""
        info = info.format(mysql_host=self.mysql_server.mysql_host, database_name=self.database_name,
                           source_table_name=self.source_table_name,
                           transfer_condition=self.transfer_condition)
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

    def loop_transfer_data(self):
        if self.table_primary_key_type == "INT":
            self.loop_transfer_data_int()
        elif self.table_primary_key_type == "CHAR":
            self.loop_transfer_data_char()

    def check_config(self):
        try:
            if str(self.database_name).strip() == "":
                PrintHelper.print_warning_message("数据库名不能为空")
                return False
            if str(self.source_table_name).strip() == "":
                PrintHelper.print_warning_message("表名不能为空")
                return False
            if str(self.transfer_condition).strip() == "":
                PrintHelper.print_warning_message("迁移条件不能为空")
                return False
            source_columns_info_list = self.get_column_info_list(
                self.source_table_name)
            column_count = len(source_columns_info_list)
            primary_key_count = 0
            for column_id in range(column_count):
                source_column_name, source_column_key, source_column_type = source_columns_info_list[
                    column_id]
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

    def transfer_data(self):
        if not self.check_config():
            return
        confirm_result = self.user_confirm()
        if confirm_result:
            self.loop_transfer_data()


def main():
    mysql_server = MysqlServer(
        mysql_host="127.0.0.1",
        mysql_port=3306,
        mysql_user="root",
        mysql_password="root",
        mysql_charset="utf8",
        database_name="testdb"
    )
    
    mysql_transfer = MyDeleter(
        mysql_server=mysql_server,
        database_name="testdb",
        source_table_name="table_name",
        transfer_condition="id<2000000",
        transfer_batch_rows=1000,
        batch_sleep_seconds=0.1,
        is_dry_run=False,
        is_user_confirm=False
    )
    mysql_transfer.transfer_data()


if __name__ == '__main__':
    main()
