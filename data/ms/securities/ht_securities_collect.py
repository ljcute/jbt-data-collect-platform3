#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/18 13:48
# 华泰证券
import os
import sys
import concurrent.futures
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.proxy_utils import get_proxies
from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder
import json
import time
from constants import *
from utils.logs_utils import logger
import datetime

collect_count_then_persist = 0  # 采集次数>collect_count_then_persist,如果采到的数据一模一样,才进行持久化

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '华泰证券'
url_ = 'https://www.htsc.com.cn/browser/rzrqPool/getBdZqc.do'


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '华泰证券'

    def rzrq_underlying_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        self.url = 'https://www.htsc.com.cn/browser/rzrqPool/getBdZqc.do'
        self.data_list = []
        data = {'date': search_date, 'hsPage': 1, 'hsPageSize': 8, 'ssPage': 1, 'ssPageSize': 8}
        # proxies = get_proxies(3, 10)
        proxies = self.get_proxies()
        response = requests.post(url=self.url, params=data, proxies=proxies, timeout=6)
        if response.status_code == 200:
            text = json.loads(response.text)
            db_hs_count = text['result']['bdHsCount']
            db_ss_count = text['result']['bdSsCount']
            self.total_num = int(db_hs_count) + int(db_ss_count)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_list = []
            about_total_page = 300
            partition_page = 50
            for i in range(0, about_total_page, partition_page):
                start_page = i + 1
                if i >= about_total_page - partition_page:
                    end_page = None  # 最后一页，设为None
                else:
                    end_page = i + partition_page

                future = executor.submit(self.collect_by_partition_rzrq, start_page, end_page, search_date, proxies)
                future_list.append(future)

            for r in future_list:
                data_list = r.result()
                if data_list:
                    self.data_list.extend(data_list)
                    self.collect_num = len(self.data_list)

    def collect_by_partition_rzrq(self, start_page, end_page, search_date, proxies):
        self.url = 'https://www.htsc.com.cn/browser/rzrqPool/getBdZqc.do'
        data_list = []
        hs_page = ss_page = start_page
        page_size = 8
        if end_page is None:
            end_page = 100000

        hs_is_continue = ss_is_continue = True
        retry_count = 5
        while (hs_is_continue or ss_is_continue) and hs_page <= end_page and ss_page <= end_page:
            data = {'date': search_date, 'hsPage': hs_page, 'hsPageSize': page_size, 'ssPage': ss_page,
                    'ssPageSize': page_size}
            logger.info("{}".format(data))
            try:
                # proxies = get_proxies(3, 10)
                response = requests.post(url=self.url, params=data, proxies=proxies, timeout=6)
                text = json.loads(response.text)
                hs_data_list = text['result']['bdHs']
                ss_data_list = text['result']['bdSs']
            except Exception as e:
                hs_is_continue = ss_is_continue = False
                if retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if hs_is_continue and len(hs_data_list) > 0:
                for i in hs_data_list:
                    # market = '沪市'
                    # stock_code = i['stockCode']
                    # stock_name = i['stockName']
                    # rz_rate = i['finRatio']
                    # rq_rate = i['sloRatio']
                    data_list.append(i)

            if ss_is_continue and len(ss_data_list) > 0:
                for i in ss_data_list:
                    # market = '深市'
                    # stock_code = i['stockCode']
                    # stock_name = i['stockName']
                    # rz_rate = i['finRatio']
                    # rq_rate = i['sloRatio']
                    data_list.append(i)

            if len(hs_data_list) == page_size:
                hs_page = hs_page + 1
            else:
                hs_is_continue = False

            if len(ss_data_list) == page_size:
                ss_page = ss_page + 1
            else:
                ss_is_continue = False

        return data_list

    def guaranty_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        self.url = 'https://www.htsc.com.cn/browser/rzrqPool/getDbZqc.do'
        self.data_list = []
        params = {"hsPageSize": 8, "hsPage": 1,
                  "ssPageSize": 8, "ssPage": 1, "date": search_date, "stockCode": None}
        proxies = self.get_proxies()
        response = requests.post(url=self.url, params=params, proxies=proxies, timeout=6)
        if response.status_code == 200:
            text = json.loads(response.text)
            db_hs_count = text['result']['dbHsCount']
            db_ss_count = text['result']['dbSsCount']
            self.total_num = int(db_hs_count) + int(db_ss_count)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_list = []
            about_total_page = 1600
            partition_page = 200
            for i in range(0, about_total_page, partition_page):
                start_page = i + 1
                if i >= about_total_page - partition_page:
                    end_page = None  # 最后一页，设为None
                else:
                    end_page = i + partition_page
                future = executor.submit(self.collect_by_partition, start_page, end_page, search_date, proxies)
                future_list.append(future)

            for r in future_list:
                data_list = r.result()
                if data_list:
                    self.data_list.extend(data_list)
                    self.collect_num = len(self.data_list)


    def collect_by_partition(self, start_page, end_page, search_date, proxies):
        url = 'https://www.htsc.com.cn/browser/rzrqPool/getDbZqc.do'
        data_list = []
        hs_page = ss_page = start_page
        page_size = 8
        if end_page is None:
            end_page = 100000

        hs_is_continue = ss_is_continue = True
        retry_count = 5
        while (hs_is_continue or ss_is_continue) and hs_page <= end_page and ss_page <= end_page:
            params = {'date': search_date, 'hsPage': hs_page, 'hsPageSize': page_size, 'ssPage': ss_page,
                      'ssPageSize': page_size}
            logger.info("{}".format(params))
            try:
                response = requests.post(url=self.url, params=params, proxies=proxies, timeout=6)
                if response is None or response.status_code != 200:
                    raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
                text = json.loads(response.text)
                hs_data_list = text['result']['dbHs']
                ss_data_list = text['result']['dbSs']
            except Exception as e:
                if retry_count > 0:
                    time.sleep(5)
                    continue

            if hs_is_continue and len(hs_data_list) > 0:
                for i in hs_data_list:
                    # market = '沪市'
                    # stock_code = i['stockCode']
                    # stock_name = i['stockName']
                    # rate = i['assureRatio']
                    # stock_group_name = i['stockGroupName']
                    data_list.append(i)

            if ss_is_continue and len(ss_data_list) > 0:
                for i in ss_data_list:
                    # market = '深市'
                    # stock_code = i['stockCode']
                    # stock_name = i['stockName']
                    # rate = i['assureRatio']
                    # stock_group_name = i['stockGroupName']
                    data_list.append(i)

            if len(hs_data_list) == page_size:
                hs_page = hs_page + 1
            else:
                hs_is_continue = False

            if len(ss_data_list) == page_size:
                ss_page = ss_page + 1
            else:
                ss_is_continue = False

        return data_list


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(2)
    # collector.collect_data(business_type=2, search_date='2022-07-18')
    # collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')
