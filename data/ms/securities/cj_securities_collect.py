#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 13:19
# 长江证券
import os
import sys
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler, get_proxies
from utils.deal_date import ComplexEncoder
import json
import time
from constants import *
from utils.logs_utils import logger
import datetime


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '长江证券'
        self.url = 'https://www.95579.com/servlet/json'

    def rzrq_underlying_securities_collect(self):
        params = {"funcNo": "902122", "i_page": 1, "i_perpage": 10000}  # 默认查询当天
        self.data_list = []
        self.title_list = ['market', 'stock_code', 'stock_name', 'rz_rate', 'rq_rate', 'rzbd', 'rqbd']
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        data_list = text['results']
        if data_list:
            self.total_num = int(data_list[0]['total_rows'])
        for i in data_list:
            stock_code = i['stock_code']
            stock_name = i['stock_name']
            rz_rate = i['fin_ratio']
            rq_rate = i['bail_ratio']
            if i['exchange_type'] == "2":
                market = '深圳'
            elif i['exchange_type'] == "1":
                market = '上海'
            else:
                market = '北京'
            rzbd = i['rzbd']
            rqbd = i['rqbd']
            self.data_list.append((market, stock_code, stock_name, rz_rate, rq_rate, rzbd, rqbd))
            self.collect_num = int(len(self.data_list))

    def guaranty_securities_collect(self):
        params = {"funcNo": "902124", "i_page": 1, "i_perpage": 10000}  # 默认查询当天
        self.data_list = []
        self.title_list = ['market', 'stock_code', 'stock_name', 'discount_rate']
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        data_list = text['results']
        if data_list:
            self.total_num = int(data_list[0]['total_rows'])
        for i in data_list:
            stock_code = i['stock_code']
            stock_name = i['stock_name']
            discount_rate = i['assure_ratio']
            if i['exchange_type'] == "2":
                market = '深圳'
            elif i['exchange_type'] == "1":
                market = '上海'
            else:
                market = '北京'
            self.data_list.append((market, stock_code, stock_name, discount_rate))
            self.collect_num = int(len(self.data_list))


if __name__ == '__main__':
    # CollectHandler().collect_data(eval(sys.argv[1]))
    pass