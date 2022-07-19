#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/13 9:39
# @Site    : 
# @File    : remove_file.py
# @Software: PyCharm
import os
import random
import time

from utils.logs_utils import logger


def remove_file(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        logger.info("删除文件异常:{}".format(e))


def random_double(mu=0.8999999999999999, sigma=0.1000000000000001):
    """
        访问深交所时需要随机数
        :param mu:
        :param sigma:
        :return:
    """
    random_value = random.normalvariate(mu, sigma)
    if random_value < 0:
        random_value = mu
    return random_value


def random_page_size(mu=28888, sigma=78888):
    """
    获取随机分页数
    :param mu:
    :param sigma:
    :return:
    """
    random_value = random.randint(mu, sigma)  # Return random integer in range [a, b], including both end points.
    return random_value


def get_timestamp():
    return int(time.time() * 1000)
