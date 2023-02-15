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
    if isinstance(search_date, datetime):
        search_date = search_date.date()
    elif isinstance(search_date, date):
        search_date = search_date
    else:
        search_date = search_date

    search_date = str(search_date)
    timeArray = time.strptime(search_date, "%Y-%m-%d")
    timestamp = time.mktime(timeArray)
    date_stamp = int(timestamp) * 1000
    return date_stamp


def stamp_to_date(stamp):
    # stamp = 1658246400000
    # print("checkpoint：%s" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(1658246400000 / 1000)))
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stamp / 1000))


def last_work_day(search_date):
    import time, datetime  # 时间
    date = datetime.datetime.combine(search_date, datetime.time())
    # date = datetime.datetime.today()  # 今天
    w = date.weekday() + 1
    # print(w) #周日到周六对应1-7
    if w == 1:  # 如果是周一，则返回上周五
        lastworkday = (date + datetime.timedelta(days=-3)).strftime("%Y-%m-%d")
    elif 1 < w < 7:  # 如果是周二到周五，则返回昨天
        lastworkday = (date + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    elif w == 7:  # 如果是周日
        lastworkday = (date + datetime.timedelta(days=-2)).strftime("%Y-%m-%d")

    return lastworkday
