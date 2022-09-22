#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/25 14:37
# @Site    : 
# @File    : hait_securities_collect.py
# @Software: PyCharm
# 海通证券
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler, get_proxies
import json
from constants import *
import datetime


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '海通证券'
        self.url = 'https://www.htsec.com/htAPI/ht/rzrq/bdzqList'

    def rzrq_underlying_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        page_count = 1
        page_size = 100
        while True:
            params = {"pagesize": page_size, "pagecount": page_count,
                      "searchDate": str(search_date).replace('-', ''),
                      "searchCode": '', "jiaoyisuo": '', "type": 1}

            response = self.get_response(self.url, 0, get_headers(), params)
            text = json.loads(response.text)
            self.total_num = int(text['result']['allSize'])
            result_list = text['result']['data']
            for i in result_list:
                # market = i['marketCode']
                # sec_code = i['productCode']
                # sec_name = i['productName']
                # if_rz = i['ifFinancing']
                # if_rq = i['ifGuarantee']
                self.data_list.append(i)
                self.collect_num = int(len(self.data_list))
            if self.total_num is not None and self.total_num > page_count * page_size:
                page_count = page_count + 1
            else:
                break

    def guaranty_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        page_count = 1
        page_size = 100
        while True:
            params = {"pagesize": page_size, "pagecount": page_count,
                      "searchDate": str(search_date).replace('-', ''),
                      "searchCode": '', "jiaoyisuo": '', "type": 2}

            response = self.get_response(self.url, 0, get_headers(), params)
            text = json.loads(response.text)
            self.total_num = int(text['result']['allSize'])
            result_list = text['result']['data']
            for i in result_list:
                # market = i['marketCode']
                # sec_code = i['productCode']
                # sec_name = i['productName']
                # rate = i['stockConvertRate']
                self.data_list.append(i)
                self.collect_num = int(len(self.data_list))
            if self.total_num is not None and self.total_num > page_count * page_size:
                page_count = page_count + 1
            else:
                break

if __name__ == '__main__':
    # collector = CollectHandler()
    # collector.collect_data(3)
    # if len(sys.argv) > 2:
    #     collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    # elif len(sys.argv) == 2:
    #     collector.collect_data(eval(sys.argv[1]))
    # elif len(sys.argv) < 2:
    #     raise Exception(f'business_type为必输参数')
    # collector.collect_data(2)
    pass