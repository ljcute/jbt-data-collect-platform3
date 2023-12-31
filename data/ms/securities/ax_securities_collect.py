#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/9/5 15:25
# @Site    : 
# @File    : ax_securities_collect.py
# @Software: PyCharm
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
from data.ms.basehandler import BaseHandler, get_headers, logger, argv_param_invoke


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '安信证券'
        self.page_size = 10
        self._proxies = self.get_proxies()

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.essence.com.cn/api/security?fislFlag=1'
        self._securities_collect()

    def guaranty_securities_collect(self):
        self.url = 'https://www.essence.com.cn/api/security?market=&name=&collatRatio=&riskLvl='
        self._securities_collect()

    def _securities_collect(self):
        response = self.get_response(self.url, 0, get_headers(), {'pageNo': 1})
        text = json.loads(response.text)
        result_list = text['data']
        if result_list:
            self.total_num = int(text['total'])
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                self.total_page = math.ceil(self.total_num / self.page_size)
                step = 10
                for _page in range(1, self.total_page + 1, step):
                    future_list = []
                    for _i in range(0, step):
                        if _page + _i > self.total_page:
                            break
                        future = executor.submit(self.collect_by_page, _page + _i)
                        future_list.append(future)
                    for r in future_list:
                        __page, df = r.result()
                        _count = 1
                        while df.empty:
                            try:
                                self.refresh_proxies(self._proxies)
                                __page, df = self.collect_by_page(__page)
                            except Exception as e:
                                _count += 1
                                if _count > 10:
                                    raise f"采集{_page}页数据失败!"
                                time.sleep(10 * (_count - 1) + 1)
                        logger.info(f" end target_page = {__page}/{self.total_page}, df_size: {df.index.size}")
                        self.tmp_df = pd.concat([self.tmp_df, df])
            self.collect_num = self.tmp_df.index.size
            self.data_text = self.tmp_df.to_csv(index=False)

    def collect_by_page(self, target_page):
        retry_count = 5
        params = {'pageNo': target_page}
        logger.info(f" start target_page = {target_page}/{self.total_page}, params: {params}")
        while retry_count:
            try:
                if retry_count < 5:
                    self.refresh_proxies(_proxies)
                _proxies = self._proxies
                response = requests.get(url=self.url, params=params, headers=get_headers(), proxies=_proxies, timeout=6)
                if response is None or response.status_code != 200:
                    raise Exception(f'{self.data_source}数据采集任务请求响应获取异常,已获取代理ip为:{self._proxies}，请求url为:{self.url},请求参数为:{params}')
                text = json.loads(response.text)
                result = text['data']
                df = pd.DataFrame(result)
                return target_page, df
            except Exception as e:
                retry_count -= 1
                time.sleep(5)
        return target_page, pd.DataFrame()


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
