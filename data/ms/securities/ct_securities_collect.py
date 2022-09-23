#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 13:19
# 财通证券
import os
import sys
import json

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler
from constants import *


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '财通证券'

    def rzrq_underlying_securities_collect(self):
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '63',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': 'Hm_lvt_2f65d6872cec3a2a39813c2c9c9126bb=1656386163; '
                      'Hm_lpvt_2f65d6872cec3a2a39813c2c9c9126bb=1656577504',
            'Host': 'www.ctsec.com',
            'Origin': 'https://www.ctsec.com',
            'Referer': 'https://www.ctsec.com/business/financing/equity',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': random.choice(USER_AGENTS),
            'X-Requested-With': 'XMLHttpRequest'
        }
        data = {
            'init_date': "2022/07/14",
            'page': 1,
            'size': 10000
        }
        self.url = 'https://www.ctsec.com/business/equityList'
        self.data_list = []
        self.title_list = ['sec_code', 'sec_name', 'rz_rate', 'rq_rate', 'market']
        response = self.get_response(self.url, 1, headers, None, data)
        text = json.loads(response.text)
        self.total_num = int(text['data']['total'])
        data = text['data']['rows']
        if data:
            for i in data:
                sec_code = i['STOCK_CODE']
                sec_name = i['STOCK_NAME']
                rz_rate = i['FIN_RATIO']
                rq_rate = i['SLO_RATIO']
                if i['EXCHANGE_TYPE'] == "2":
                    market = '深圳'
                elif i['EXCHANGE_TYPE'] == "1":
                    market = '上海'
                else:
                    market = '北京'
                self.data_list.append((sec_code, sec_name, rz_rate, rq_rate, market))
                self.collect_num = int(len(self.data_list))

    def guaranty_securities_collect(self):
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '71',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': 'Hm_lvt_2f65d6872cec3a2a39813c2c9c9126bb=1656386163; '
                      'Hm_lpvt_2f65d6872cec3a2a39813c2c9c9126bb=1656577524',
            'Host': 'www.ctsec.com',
            'Origin': 'https://www.ctsec.com',
            'Referer': 'https://www.ctsec.com/business/financing/butFor',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': random.choice(USER_AGENTS),
            'X-Requested-With': 'XMLHttpRequest'
        }
        data = {
            'init_date': "2022/07/14",
            'page': 1,
            'size': 10000
        }
        self.url = 'https://www.ctsec.com/business/getAssureList'
        self.data_list = []
        self.title_list = ['sec_code', 'sec_name', 'discount_rate', 'stock_group_name', 'stock_type', 'market']
        response = self.get_response(self.url, 1, headers, None, data)
        text = json.loads(response.text)
        self.total_num = int(text['data']['total'])
        data = text['data']['rows']
        if data:
            for i in data:
                sec_code = i['STOCK_CODE']
                sec_name = i['STOCK_NAME']
                discount_rate = i['ASSURE_RATIO']  # 融资保证金比例
                stock_group_name = i['stockgroup_no']  # 1为A组 ，none为B组 ，4为C组， 5为D组，6为E，7为F
                stock_type = i['STOCK_TYPE']
                market = i['MARKET']  # 融券保证金比例
                self.data_list.append((sec_code, sec_name, discount_rate, stock_group_name, stock_type, market))
                self.collect_num = int(len(self.data_list))


if __name__ == '__main__':
    # CollectHandler().collect_data(eval(sys.argv[1]))
    pass

