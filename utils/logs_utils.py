#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description : 日志工具类
@File        : __init__.py
@Project     : jbt-data-collect-platform
@Date        : 2022-9-19
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""

__author__ = 'Eagle (liuzh@igoldenbeta.com)'

from logging import handlers
import logging
from config import Config


def get_log_info():
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }
    filename = Config().get_content("log_file").get("filename")
    # filename = 'logs/logs.log'
    logger = logging.getLogger(filename)
    fmt = '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
    format_str = logging.Formatter(fmt)
    logger.setLevel(level_relations.get('info'))
    # handler = logging.FileHandler(log_path)
    sh = logging.StreamHandler()
    sh.setFormatter(format_str)
    th = handlers.TimedRotatingFileHandler(filename=filename, when='MIDNIGHT', interval=1, backupCount=7, encoding='utf-8')
    th.setFormatter(format_str)
    logger.addHandler(sh)
    logger.addHandler(th)
    # logger.addHandler(handler)
    return logger


logger = get_log_info()
