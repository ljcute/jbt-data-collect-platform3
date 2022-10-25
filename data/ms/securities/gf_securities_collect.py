#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/21 13:58

import os
import sys
import json
import time
import math
import requests
import pandas as pd
import concurrent.futures

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, get_headers, logger


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '广发证券'
        self.page_size = 20
        self.total_page = 0
        self.init_date = None
        self._proxies = self.get_proxies()

    def rz_underlying_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self.url = 'http://www.gf.com.cn/business/finance/targetlist'
        self._securities_collect('rz')

    def rq_underlying_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self.url = 'http://www.gf.com.cn/business/finance/targetlist'
        self._securities_collect('rq')

    def guaranty_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self.url = 'http://www.gf.com.cn/business/finance/ratiolist'
        self._securities_collect('db')

    def _get_params(self, biz_type, target_page):
        params = {"pageSize": self.page_size, "pageNum": target_page, 'dir': 'asc', 'init_date': self.init_date,
                  'sort': 'init_date', 'key': None}
        if biz_type == 'db':
            pass
        elif biz_type == 'rq':
            params["type"] = 'slo'
        elif biz_type == 'rz':
            params["type"] = 'fin'
        return params

    def _securities_collect(self, biz_type):
        params = self._get_params(biz_type, 1)
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['count'])
        result = text['result']
        if self.total_num == 0 and result.index('当前搜索记录为空') > 0:
            self.collect_num_check = False
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_list = []
            self.total_page = math.ceil(self.total_num / self.page_size)
            for target_page in range(1, self.total_page + 1):
                future = executor.submit(self.collect_by_page, biz_type, target_page)
                future_list.append(future)
            for r in future_list:
                target_page, df = r.result()
                logger.info(f" end target_page = {target_page}/{self.total_page}, df_size: {df.index.size}")
                self.tmp_df = pd.concat([self.tmp_df, df])
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_string()

    def collect_by_page(self, biz_type, target_page):
        retry_count = 5
        params = self._get_params(biz_type, target_page)
        logger.info(f" start target_page = {target_page}/{self.total_page}, params: {params}")
        while retry_count:
            try:
                _proxies = self._proxies
                response = requests.get(url=self.url, params=params, headers=get_headers(), proxies=_proxies, timeout=6)
                if response is None or response.status_code != 200:
                    raise Exception(f'{self.data_source}数据采集任务请求响应获取异常,已获取代理ip为:{self._proxies}，请求url为:{self.url},请求参数为:{params}')
                text = json.loads(response.text)
                result = text['result']
                df = pd.read_html('<table>' + result + '</table>')[0]
                return target_page, df
            except Exception as e:
                retry_count -= 1
                time.sleep(5)
                self.refresh_proxies(_proxies)
        return target_page, pd.DataFrame()


if __name__ == '__main__':
    CollectHandler().argv_param_invoke((2, 4, 5), sys.argv)
