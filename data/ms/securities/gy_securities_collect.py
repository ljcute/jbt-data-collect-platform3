#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/20 14:21
# 国元证券
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler
import json
from constants import *
import datetime


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '国元证券'
        self.url = 'http://www.gyzq.com.cn/servlet/json'

    def rz_underlying_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        # cxlx 0融资,1融券
        data = {
            'date': search_date,
            'funcNo': 904103,
            'cxlx': 0
        }
        response = self.get_response(self.url, 2, get_headers(), None, data)
        text = json.loads(response.text)
        all_data_list = text['results']
        for i in all_data_list:
            # stock_name = i['secu_name']
            # stock_code = i['secu_code']
            # rz_rate = i['rz_ratio']
            # market = '深圳' if str(i['stkex']) == '0' else '上海'
            self.data_list.append(i)
            self.collect_num = len(self.data_list)
        self.total_num = len(self.data_list)


    def rq_underlying_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        # cxlx 0融资,1融券
        data = {
            'date': search_date,
            'funcNo': 904103,
            'cxlx': 1
        }
        # response = session.post(url=url, data=data, headers=get_headers(), proxies=proxies)
        response = self.get_response(self.url, 2, get_headers(), None, data)
        text = json.loads(response.text)
        all_data_list = text['results']
        for i in all_data_list:
            # stock_name = i['secu_name']
            # stock_code = i['secu_code']
            # rq_rate = i['rq_ratio']
            # market = '深圳' if str(i['stkex']) == '0' else '上海'
            self.data_list.append(i)
            self.collect_num = len(self.data_list)
        self.total_num = len(self.data_list)


    def guaranty_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        # cxlx 0融资,1融券
        data = {
            'date': search_date,
            'funcNo': 904104
        }
        # response = session.post(url=url, data=data, headers=get_headers(), proxies=proxies)
        response = self.get_response(self.url, 2, get_headers(), None, data)
        text = json.loads(response.text)
        all_data_list = text['results']
        for i in all_data_list:
            # stock_name = i['secu_name']
            # stock_code = i['secu_code']
            # exchange_rate = i['exchange_rate']
            # market = '深圳' if str(i['stkex']) == '0' else '上海'
            self.data_list.append(i)
            self.collect_num = len(self.data_list)
        self.total_num = len(self.data_list)



if __name__ == '__main__':
    collector = CollectHandler()
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')
