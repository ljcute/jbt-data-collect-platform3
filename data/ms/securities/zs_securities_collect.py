#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 10:19
# 招商证券 --interface
import os
import sys
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder
import json
from constants import *
from utils.logs_utils import logger
import datetime

url_ = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstock'


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '招商证券'

    def rz_underlying_securities_collect(self):
        page_size = random_page_size()
        params = {"pageSize": page_size, "pageNum": 1, "rqbdflag": 1}  # rqbdflag = 1融资
        self.url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstock'
        self.data_list = []
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['body']['totalNum'])
        data = text['body']['stocks']
        for i in data:
            # stock_code = i['stkcode']
            # stock_name = i['stkname']
            # margin_rate = i['marginratefund']
            # market = '沪市' if i['market'] == '1' else '深市'
            self.data_list.append(i)
            self.collect_num = len(self.data_list)

    def rq_underlying_securities_collect(self):
        page_size = random_page_size()
        params = {"pageSize": page_size, "pageNum": 1, "rqbdflag": 2}  # rqbdflag = 1融资,2融券
        self.url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstock'
        self.data_list = []
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['body']['totalNum'])
        data = text['body']['stocks']
        for i in data:
            # stock_code = i['stkcode']
            # stock_name = i['stkname']
            # margin_rate = i['marginratefund']
            # market = '沪市' if i['market'] == '1' else '深市'
            self.data_list.append(i)
            self.collect_num = len(self.data_list)

    def guaranty_securities_collect(self):
        page_size = random_page_size()
        params = {"pageSize": page_size, "pageNum": 1}
        self.url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstockdiscount'
        self.data_list = []
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['body']['totalNum'])
        data = text['body']['stocks']
        for i in data:
            # stock_code = i['stkcode']
            # stock_name = i['stkname']
            # margin_rate = i['pledgerate']
            # market = '沪市' if i['market'] == '1' else '深市'
            self.data_list.append(i)
            self.collect_num = len(self.data_list)


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
    # collector.collect_data(5)
    collector.collect_data(eval(sys.argv[1]))
