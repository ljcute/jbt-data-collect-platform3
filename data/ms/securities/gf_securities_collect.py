#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/21 13:58
# 广发证券

import os
import sys
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from bs4 import BeautifulSoup
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder
import json
import time
from constants import *
from utils.logs_utils import logger
import datetime

url_ = 'http://www.gf.com.cn/business/finance/targetlist'
url__ = 'http://www.gf.com.cn/business/finance/targetlist'
url___ = 'http://www.gf.com.cn/business/finance/ratiolist'


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '广发证券'

    def rz_underlying_securities_collect(self):
        page = 1
        page_size = 20
        retry_count = 3
        is_continue = True
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        self.url = 'http://www.gf.com.cn/business/finance/targetlist'
        self.data_list = []
        self.title_list = ['stock_name', 'stock_code', 'rate', 'date']
        while is_continue:
            params = {"pageSize": page_size, "pageNum": page, "type": 'fin', 'dir': 'asc', 'init_date': search_date,
                      'sort': 'init_date', 'key': None}
            response = self.get_response(self.url, 0, get_headers(), params)
            text = json.loads(response.text)
            self.total_num = int(text['count'])
            result = text['result']
            soup = BeautifulSoup(result, 'html.parser')
            dom_td_list = soup.select('td')
            for i in range(0, len(dom_td_list) - 1, 4):
                dom_span_list = dom_td_list[i].find_all('span')
                stock_name = dom_span_list[0].get_text()
                stock_code = dom_span_list[1].get_text()
                rate = dom_td_list[i + 1].get_text()
                self.biz_dt = dom_td_list[i + 2].get_text()
                self.data_list.append((stock_name, stock_code, rate, self.biz_dt))
                self.collect_num = int(len(self.data_list))
            if self.total_num is not None and type(self.total_num) is not str and self.total_num > page * page_size:
                is_continue = True
                page = page + 1
            else:
                if (len(result) == 0 or self.total_num == 0) and retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(3)
                    continue
                is_continue = False

    def rq_underlying_securities_collect(self):
        page = 1
        page_size = 20
        is_continue = True
        retry_count = 3
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        self.url = 'http://www.gf.com.cn/business/finance/targetlist'
        self.data_list = []
        self.title_list = ['stock_name', 'stock_code', 'rate', 'date']
        while is_continue:
            params = {"pageSize": page_size, "pageNum": page, "type": 'slo', 'dir': 'asc', 'init_date': search_date,
                      'sort': 'init_date', 'key': None}
            response = self.get_response(self.url, 0, get_headers(), params)
            text = json.loads(response.text)
            self.total_num = int(text['count'])
            result = text['result']
            soup = BeautifulSoup(result, 'html.parser')
            dom_td_list = soup.select('td')
            for i in range(0, len(dom_td_list) - 1, 4):
                dom_span_list = dom_td_list[i].find_all('span')
                stock_name = dom_span_list[0].get_text()
                stock_code = dom_span_list[1].get_text()
                rate = dom_td_list[i + 1].get_text()
                self.biz_dt = dom_td_list[i + 2].get_text()
                self.data_list.append((stock_name, stock_code, rate, self.biz_dt))
                self.collect_num = int(len(self.data_list))
            if self.total_num is not None and type(self.total_num) is not str and self.total_num > page * page_size:
                is_continue = True
                page = page + 1
            else:
                if (len(result) == 0 or self.total_num == 0) and retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(3)
                    continue
                is_continue = False

    def guaranty_securities_collect(self):
        page = 1
        page_size = 20
        is_continue = True
        total = None
        retry_count = 3
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        self.url = 'http://www.gf.com.cn/business/finance/ratiolist'
        self.data_list = []
        self.title_list = ['stock_name', 'stock_code', 'rate', 'date']
        while is_continue:
            params = {"pageSize": page_size, "pageNum": page, 'dir': 'asc', 'init_date': search_date,
                      'sort': 'init_date', 'key': None}
            response = self.get_response(self.url, 0, get_headers(), params)
            text = json.loads(response.text)
            self.total_num = int(text['count'])
            result = text['result']
            soup = BeautifulSoup(result, 'html.parser')
            dom_td_list = soup.select('td')
            for i in range(0, len(dom_td_list) - 1, 4):
                dom_span_list = dom_td_list[i].find_all('span')
                stock_name = dom_span_list[0].get_text()
                stock_code = dom_span_list[1].get_text()
                rate = dom_td_list[i + 1].get_text()
                self.biz_dt = dom_td_list[i + 2].get_text()
                self.data_list.append((stock_name, stock_code, rate, self.biz_dt))
                self.collect_num = int(len(self.data_list))
                logger.info(f'已采集数据条数为：{int(len(self.data_list))}')

            if self.total_num is not None and type(self.total_num) is not str and self.total_num > page * page_size:
                is_continue = True
                page = page + 1
            else:
                if (len(result) == 0 or self.total_num == 0) and retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(3)
                    continue
                is_continue = False


if __name__ == '__main__':
    # collector = CollectHandler()
    # collector.collect_data(4)
    # collector.collect_data(4)
    # if len(sys.argv) > 2:
    #     CollectHandler.collect_data(eval(sys.argv[1]), sys.argv[2])
    # elif len(sys.argv) == 2:
    #     CollectHandler.collect_data(eval(sys.argv[1]))
    # elif len(sys.argv) < 2:
    #     raise Exception(f'business_type为必输参数')
    pass
