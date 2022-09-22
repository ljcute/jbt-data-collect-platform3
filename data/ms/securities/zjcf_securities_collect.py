#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/9/19 10:41
# @Site    : 
# @File    : zjcf_securities_collect.py
# @Software: PyCharm
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler, get_proxies
import json
from constants import *


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '中金财富'
        self.url = 'https://www.cicc.com/ciccdata/reportservice/showreportdata.do'

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.ciccwm.com/api2/web2016/cicc/rzrq/getTargetStockInfo?'
        self.data_list = []
        page = 1
        size = random_page_size()
        params = {'pageNum': page, 'pageSize': size}
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['data']['total'])

        data = text['data']['list']
        for i in data:
            # exchname = i['exchname']
            # stkid = i['stkid']
            # groupid = i['groupid']
            # stkname = i['stkname']
            # stktypename = i['stktypename']
            # rzbd = i['iscreditcashstk']
            # rz_rate = i['ccmarginrate']
            # rqbd = i['iscreditsharestk']
            # rq_rate = i['csmarginrate']
            # bzj = i['iscmo']
            # bzj_rate = i['cmorate']
            # fairprice = i['fairprice']
            self.data_list.append(i)
            self.collect_num = int(len(self.data_list))

    def guaranty_securities_collect(self):
        self.url = 'https://www.ciccwm.com/api2/web2016/cicc/rzrq/getRzrqGuaranteeInfo?'
        self.data_list = []
        page = 1
        size = random_page_size()
        params = {'pageNum': page, 'pageSize': size}
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['data']['total'])
        data = text['data']['list']
        for i in data:
            self.data_list.append(i)
            self.collect_num = int(len(self.data_list))


def random_page_size(mu=28888, sigma=78888):
    """
    获取随机分页数
    :param mu:
    :param sigma:
    :return:
    """
    random_value = random.randint(mu, sigma)  # Return random integer in range [a, b], including both end points.
    return random_value


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(3)
    collector.collect_data(eval(sys.argv[1]))
