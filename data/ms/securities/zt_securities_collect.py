#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/20 15:42
# 中泰证券
import concurrent.futures
import math
import os
import sys

from pandas.core._numba import executor

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder, date_to_stamp
import json
import time
from constants import *
from utils.logs_utils import logger
import datetime


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '中泰证券'
        self.url = 'https://www.95538.cn/rzrq/data/Handler.ashx'

    def rzrq_underlying_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        page = 1
        params = {"action": "GetBdstockNoticesPager", "pageindex": page, "date": date_to_stamp(search_date)}
        headers = get_headers()
        headers['Referer'] = 'https://www.95538.cn/rzrq/rzrq1.aspx?action=GetEarnestmoneyNoticesPager&tab=3&keyword='

        response = self.get_response(self.url, 0, headers, params)
        text = json.loads(response.text)
        self.total_num = int(text['PageTotal'])
        for curr_page in range(1, self.total_num + 1):
            logger.info(f'当前为第{curr_page}页')
            params = {"action": "GetBdstockNoticesPager", "pageindex": curr_page,
                      "date": date_to_stamp(search_date)}
            response = self.get_response(self.url, 0, headers, params)
            text = json.loads(response.text)
            all_data_list = text['Items']
            if all_data_list:
                for i in all_data_list:
                    # stock_code = i['STOCK_CODE']
                    # stock_name = i['STOCK_NAME']
                    # rz_rate = i['FUND_RATIOS']
                    # rq_rate = i['STOCK_RATIOS']
                    # status = i['STOCK_STATE']
                    # date = search_date
                    # note = i['NOTE']
                    self.data_list.append(i)
                    self.collect_num = len(self.data_list)

    def guaranty_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        while True:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_list = []
                about_total_page = 2500
                partition_page = 250
                for i in range(0, about_total_page, partition_page):
                    start_page = i + 1
                    if i >= about_total_page - partition_page:
                        end_page = None  # 最后一页
                    else:
                        end_page = i + partition_page

                    logger.info(f'这是第{i}此循环')
                    future = executor.submit(self.collect_by_partition_rz, start_page, end_page, search_date, self.url,
                                             timeout=1800)
                    future_list.append(future)
                for r in future_list:
                    params, data_list = r.result()
                    if data_list:
                        self.data_list.extend(data_list)
                        self.collect_num = len(self.data_list)

    def collect_by_partition_rz(self, start_page, end_page, search_date, url):
        params = (start_page, end_page, search_date, url)
        data_list = []
        all_data_list = []
        if end_page is None:
            end_page = 100000

        is_continue = True
        retry_count = 3
        while is_continue and start_page <= end_page:
            params = {"action": "GetEarnestmoneyNoticesPager", "pageindex": start_page,
                      "date": date_to_stamp(search_date)}
            headers = get_headers()
            headers[
                'Referer'] = 'https://www.95538.cn/rzrq/rzrq1.aspx?action=GetEarnestmoneyNoticesPager&tab=3&keyword='

            try:
                # response = super().get_response(data_source, url, proxies, 0, headers, params)
                response = self.get_response(url, 0, headers, params)
                text = json.loads(response.text)
                all_data_list = text['Items']
            except Exception as e:
                if retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue
                else:
                    is_continue = False

            if is_continue and len(all_data_list) > 0:
                for i in all_data_list:
                    # stock_code = i['STOCK_CODE']
                    # stock_name = i['STOCK_NAME']
                    # rate = i['REBATE']
                    # date = search_date
                    # note = i['NOTE']
                    data_list.append(i)
                    self.collect_num = len(data_list)

            if int(len(all_data_list)) == 10:
                start_page = start_page + 1
            else:
                is_continue = False

        logger.info(f'此线程采集数据共datalist:{len(data_list)}条')
        return data_list


if __name__ == '__main__':
    # collector = CollectHandler()
    # if len(sys.argv) > 2:
    #     collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    # elif len(sys.argv) == 2:
    #     collector.collect_data(eval(sys.argv[1]))
    # elif len(sys.argv) < 2:
    #     raise Exception(f'business_type为必输参数')
    pass