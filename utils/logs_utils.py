#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/23 13:42
from logging import handlers
import logging
import os


def get_log_info():
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }

    filename = '../../../logs/logs.log'
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
