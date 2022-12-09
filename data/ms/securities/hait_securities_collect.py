#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/25 14:37
# @Site    : 
# @File    : hait_securities_collect.py
# @Software: PyCharm
# 海通证券
import math
import os
import sys
import time

import pandas as pd
import json
import datetime
import concurrent.futures

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler, get_headers, logger, argv_param_invoke
from constants import *


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '海通证券'
        self.page_size = 5000
        self.page_count = 1
        self.init_date = None
        self.url = 'https://www.htsec.com/htAPI/ht/rzrq/bdzqList'
        self._proxies = self.get_proxies()

    def rzrq_underlying_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('bd')

    def guaranty_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('db')

    def _get_params(self, biz_type, target_page):
        params = {"pagesize": self.page_size, "pagecount": target_page,
                  "searchDate": self.init_date,
                  "searchCode": '', "jiaoyisuo": ''}
        if biz_type == 'bd':
            params["type"] = 1
        elif biz_type == 'db':
            params["type"] = 2
        return params

    def _securities_collect(self, biz_type):
        params = self._get_params(biz_type, 1)
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['result']['allSize'])

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            self.total_page = math.ceil(self.total_num / self.page_size)
            step = 5
            for _page in range(1, self.total_page + 1, step):
                future_list = []
                for _i in range(0, step):
                    if _page + _i > self.total_page:
                        break
                    future = executor.submit(self.collect_by_page, biz_type, _page + _i)
                    future_list.append(future)
                for r in future_list:
                    __page, df = r.result()
                    while df.empty:
                        try:
                            self.refresh_proxies(self._proxies)
                            __page, df = self.collect_by_page(biz_type, __page)
                        except Exception as e:
                            time.sleep(1)
                    logger.info(f" end target_page = {__page}/{self.total_page}, df_size: {df.index.size}")
                    self.tmp_df = pd.concat([self.tmp_df, df])
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)

    def collect_by_page(self, biz_type, target_page):
        retry_count = 5
        params = self._get_params(biz_type, target_page)
        logger.info(f" start target_page = {target_page}/{self.total_page}, params: {params}")
        while retry_count:
            try:
                if retry_count < 5:
                    self.refresh_proxies(_proxies)
                _proxies = self._proxies
                response = requests.get(url=self.url, params=params, headers=get_headers(), proxies=_proxies,
                                        timeout=120)
                if response is None or response.status_code != 200:
                    raise Exception(
                        f'{self.data_source}数据采集任务请求响应获取异常,已获取代理ip为:{self._proxies}，请求url为:{self.url},请求参数为:{params}')
                text = json.loads(response.text)
                result = text['result']['data']
                return target_page, pd.DataFrame(result)
            except Exception as e:
                retry_count -= 1
                time.sleep(5)
                self.refresh_proxies(_proxies)
        return target_page, pd.DataFrame()


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
