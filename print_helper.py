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


class PrintHelper(object):
    working_folder = os.path.dirname(os.path.abspath(__file__))
    log_folder = os.path.join(working_folder, "logs")
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    default_log_file = os.path.join(log_folder, "default_log.txt")
    error_log_file = os.path.join(log_folder, "error_log.txt")

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
