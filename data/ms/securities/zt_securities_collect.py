#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/20 15:42
# 中泰证券
import concurrent.futures
import math
import os
import sys
import pandas as pd

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
        self.init_date = None
        self.url = 'https://www.95538.cn/rzrq/data/Handler.ashx'
        self._proxies = self.get_proxies()

    def rzrq_underlying_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('bd')

    def guaranty_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('db')

    def _get_params_headers(self, biz_type, target_page):
        action = ''
        if biz_type == 'bd':
            action = 'GetBdstockNoticesPager'
        elif biz_type == 'db':
            action = 'GetEarnestmoneyNoticesPager'
        params = {"action": action, "pageindex": target_page, "date": date_to_stamp(self.search_date)}
        headers = get_headers()
        headers['Referer'] = 'https://www.95538.cn/rzrq/rzrq1.aspx?action=GetEarnestmoneyNoticesPager&tab=3&keyword='
        return params, headers

    def _securities_collect(self, biz_type):
        params, headers = self._get_params_headers(biz_type, 1)
        response = self.get_response(self.url, 0, headers, params)
        text = json.loads(response.text)
        total_page = int(text['PageTotal'])

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_list = []
            for target_page in range(1, total_page + 1):
                future = executor.submit(self.collect_by_page, biz_type, target_page)
                future_list.append(future)

            for r in future_list:
                target_page, result_list = r.result()
                logger.info(f" end target_page = {target_page}/{total_page}, df_size: {len(result_list)}")
                if len(result_list) > 0:
                    self.tmp_df = pd.concat([self.tmp_df, pd.DataFrame(result_list)])
            self.collect_num = self.tmp_df.index.size
            self.total_num = len(result_list)
            self.data_text = self.tmp_df.to_csv(index=False)

    def collect_by_page(self, biz_type, target_page):
        retry_count = 5
        params, headers = self._get_params_headers(biz_type, target_page)
        logger.info(f" start target_page = {target_page}, params: {params}")
        while retry_count:
            try:
                _proxies = self._proxies
                response = requests.get(url=self.url, params=params, headers=headers, proxies=_proxies, timeout=6)
                if response is None or response.status_code != 200:
                    raise Exception(
                        f'{self.data_source}数据采集任务请求响应获取异常,已获取代理ip为:{self._proxies}，请求url为:{self.url},请求参数为:{params}')
                text = json.loads(response.text)
                result = text['Items']
                return target_page, result
            except Exception as e:
                retry_count -= 1
                time.sleep(5)
                self.refresh_proxies(_proxies)
        return target_page, pd.DataFrame()


if __name__ == '__main__':
    CollectHandler().argv_param_invoke((2, 3), sys.argv)
    # CollectHandler().collect_data(2)
