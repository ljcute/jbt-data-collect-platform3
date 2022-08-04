#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/13 14:25
# @Site    : 
# @File    : deal_date.py
# @Software: PyCharm
import json
import time
from datetime import date, datetime


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


# search_date = 2020-07-12
def date_to_stamp(search_date):
    search_date = str(search_date)
    timeArray = time.strptime(search_date, "%Y-%m-%d")
    timestamp = time.mktime(timeArray)
    date_stamp = int(timestamp) * 1000
    return date_stamp


def stamp_to_date(stamp):
    # stamp = 1658246400000
    # print("checkpoint：%s" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(1658246400000 / 1000)))
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stamp / 1000))
