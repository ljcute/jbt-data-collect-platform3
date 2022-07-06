#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/23 13:42

import logging


def get_console_logger(log_level=logging.INFO):
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
    console_handler.setFormatter(formatter)

    logger_ = logging.getLogger()
    logger_.addHandler(console_handler)

    logger_.setLevel(log_level)
    return logger_


# 日志
logger = get_console_logger()
