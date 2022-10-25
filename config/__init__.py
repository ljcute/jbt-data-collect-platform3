#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description : 配置文件读取
@File        : __init__.py
@Project     : jbt-data-collect-platform
@Date        : 2022-9-19
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""

__author__ = 'Eagle (liuzh@igoldenbeta.com)'

import configparser
import os


class Config(object):

    def __init__(self, config_filename="config.ini"):
        """
        读取配置文件
        """
        filepath = os.path.join(os.path.dirname(__file__), config_filename)
        self.cf = configparser.ConfigParser()
        self.cf.read(filepath, encoding='utf-8')

    def get_sections(self):
        return self.cf.sections()

    def get_options(self, section):
        return self.cf.options(section)

    def get_content(self, section):
        result = {}
        for option in self.get_options(section):
            value = self.cf.get(section, option)
            result[option] = int(value) if value.isdigit() else value
        return result


if __name__ == "__main__":
    print(f'{Config()}')
