#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/18 10:09
# 中信证券
import os
import sys
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder
import json
import time
from constants import *
from utils.logs_utils import logger
import datetime

data_source = '中信证券'
url = 'https://kong.citics.com/pub/api/v1/website/rzrq/rzrqObjects'


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '中信证券'

    def rzrq_underlying_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        self.url = 'https://kong.citics.com/pub/api/v1/website/rzrq/rzrqObjects'
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '42',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'kong.citics.com',
            'Origin': 'https://pb.citics.com',
            'Referer': 'https://pb.citics.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': random.choice(USER_AGENTS),
        }
        curr_page = 1
        data = {
            'pageSize': 20,
            'currPage': curr_page,
            'searchDate': search_date
        }
        response = self.get_response(self.url, 1, headers, None, data)
        self.data_list = []
        text = json.loads(response.text)
        if text['errorCode'] == '100008':
            logger.error(f'该日{search_date}为非交易日,无相应数据')
            raise Exception(f'该日{search_date}为非交易日,无相应数据')

        self.total_num = int(text['data']['totalRecord'])
        total_page = int(self.total_num / 20) + 1

        for curr_page in range(1, total_page + 1):
            logger.info(f'当前为第{curr_page}页')
            data = {
                'pageSize': 20,
                'currPage': curr_page,
                'searchDate': search_date
            }
            response = self.get_response(self.url, 1, headers, None, data)
            text = json.loads(response.text)
            data = text['data']['data']
            if data:
                for i in data:
                    # stock_code = i['stockCode']
                    # stock_name = i['stockName']
                    # rz_rate = i['rzPercent']
                    # rq_rate = i['rqPercent']
                    # date = i['dataDate']
                    # markert = i['exchangeCode']
                    self.data_list.append(i)
                    self.collect_num = len(self.data_list)

    def guaranty_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        self.url = 'https://kong.citics.com/pub/api/v1/website/rzrq/punching'
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '42',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'kong.citics.com',
            'Origin': 'https://pb.citics.com',
            'Referer': 'https://pb.citics.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': random.choice(USER_AGENTS),
        }
        curr_page = 1
        data = {
            'pageSize': 20,
            'currPage': curr_page,
            'searchDate': search_date
        }
        self.data_list = []
        response = self.get_response(self.url, 1, headers, None, data)
        text = json.loads(response.text)
        if text['errorCode'] == '100008':
            logger.error(f'该日{search_date}为非交易日,无相应数据')
            raise Exception(f'该日{search_date}为非交易日,无相应数据')

        self.total_num = int(text['data']['totalRecord'])
        total_page = int(self.total_num / 20) + 1

        for curr_page in range(1, total_page + 1):
            logger.info(f'当前为第{curr_page}页')
            data = {
                'pageSize': 20,
                'currPage': curr_page,
                'searchDate': search_date
            }
            response = self.get_response(self.url, 1, headers, None, data)
            text = json.loads(response.text)
            data = text['data']['data']
            if data:
                for i in data:
                    # market = i['exchangeCode']
                    # stock_code = i['stockCode']
                    # stock_name = i['stockName']
                    # rate = i['percent']
                    # date = i['dataDate']
                    # status = i['status']
                    # stockgroup_name = i['stockgroup_name']
                    self.data_list.append(i)
                    self.collect_num = len(self.data_list)

        # for curr_page in range(1, total_page + 1):
        #     logger.info(f'当前为第{curr_page}页')
        #     data = {
        #         'pageSize': 20,
        #         'currPage': curr_page,
        #         'searchDate': search_date
        #     }
        #     response = self.get_response(self.url, 1, headers, None, data)
        #     text = json.loads(response.text)
        #     data = text['data']['data']
        #     if data:
        #         for i in data:
        #             market = i['exchangeCode']
        #             stock_code = i['stockCode']
        #             stock_name = i['stockName']
        #             rate = i['percent']
        #             date = i['dataDate']
        #             status = i['status']
        #             stockgroup_name = i['stockgroup_name']
        #             self.data_list.append((market, stock_code, stock_name, rate, date, status, stockgroup_name))
        #             # self.data_list.append(i)
        #             self.collect_num = len(self.data_list)
        # print(f'长度:{len(self.data_list)}')
        # df = pd.DataFrame(data=self.data_list, columns=['市场', '证券代码', '证券简称', '折算率', '日期', '买入及转入状态', '证券集中度分组'])
        # print(df)
        # df.to_excel('中信证券-0923.xlsx')

if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(2)
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')