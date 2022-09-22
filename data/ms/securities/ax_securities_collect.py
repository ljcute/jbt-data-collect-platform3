#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/9/5 15:25
# @Site    : 
# @File    : ax_securities_collect.py
# @Software: PyCharm
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler
import json
from constants import *


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '安信证券'

    def rzrq_underlying_securities_collect(self):
        pageNo = 1
        page_size = 10
        self.url = 'https://www.essence.com.cn/api/security?fislFlag=1'
        self.data_list = []
        while True:
            params = {'pageNo': pageNo}
            response = self.get_response(self.url, 0, get_headers(), params)
            text = json.loads(response.text)
            result_list = text['data']
            if result_list:
                self.total_num = int(text['total'])
            for i in result_list:
                # if i['market'] == "0":
                #     market = '深圳'
                # elif i['market'] == "1":
                #     market = '上海'
                # else:
                #     market = '北京'
                # sec_code = i['secuCode']
                # sec_name = i['secuName']
                # rz_rate = i['fiMarginRatio']
                # rq_rate = i['slMarginRatio']
                # rz_status = i['enableFi']
                # rq_status = i['enableSl']
                # self.biz_dt = i['effectiveDate']
                self.data_list.append(i)
                self.collect_num = int(len(self.data_list))
            if self.total_num is not None and self.total_num > pageNo * page_size:
                pageNo = pageNo + 1
            else:
                break

    def guaranty_securities_collect(self):
        pageNo = 1
        page_size = 10
        self.url = 'https://www.essence.com.cn/api/security?market=&name=&collatRatio=&riskLvl='
        self.data_list = []
        while True:
            params = {'pageNo': pageNo}
            response = self.get_response(self.url, 0, get_headers(), params)
            text = json.loads(response.text)
            result_list = text['data']
            if result_list:
                self.total_num = int(text['total'])
            for i in result_list:
                # if i['market'] == "0":
                #     market = '深圳'
                # elif i['market'] == "1":
                #     market = '上海'
                # else:
                #     market = '北京'
                # sec_code = i['secuCode']
                # sec_name = i['secuName']
                # round_rate = i['collatRatio']
                # self.biz_dt = i['effectiveDate']
                self.data_list.append(i)
                self.collect_num = int(len(self.data_list))
            if self.total_num is not None and self.total_num > pageNo * page_size:
                pageNo = pageNo + 1
            else:
                break


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(3)
    collector().collect_data(eval(sys.argv[1]))

