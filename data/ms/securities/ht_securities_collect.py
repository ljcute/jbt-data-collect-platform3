#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/18 13:48
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
from data.ms.basehandler import BaseHandler, logger, argv_param_invoke


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '华泰证券'
        self.page_size = 8
        self._proxies = self.get_proxies()

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.htsc.com.cn/browser/rzrqPool/getBdZqc.do'
        self._securities_collect("bd")

    def guaranty_securities_collect(self):
        self.url = 'https://www.htsc.com.cn/browser/rzrqPool/getDbZqc.do'
        self._securities_collect("db")

    def _securities_collect(self, bizCode):
        params = {"hsPageSize": self.page_size, "hsPage": 1,
                  "ssPageSize": self.page_size, "ssPage": 1,
                  "bsPageSize": self.page_size, "bsPage": 1, "date": self.search_date.strftime('%Y-%m-%d'), "stockCode": None}
        response = requests.post(url=self.url, params=params, proxies=self._proxies, timeout=6)
        if response.status_code == 200:
            text = json.loads(response.text)
            hs_count = int(text['result'][f'{bizCode}HsCount'])
            ss_count = int(text['result'][f'{bizCode}SsCount'])
            bs_count = int(text['result'][f'{bizCode}BsCount'])
            self.total_num = hs_count + ss_count + bs_count

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            self.total_page = max(math.ceil(hs_count/self.page_size), math.ceil(ss_count/self.page_size), math.ceil(bs_count/self.page_size))
            step = 5
            for _page in range(1, self.total_page + 1, step):
                future_list = []
                for _i in range(0, step):
                    if _page + _i > self.total_page:
                        break
                    future = executor.submit(self.collect_by_page, _page + _i, bizCode)
                    future_list.append(future)
                for r in future_list:
                    __page, hs_data_list, ss_data_list, bs_data_list = r.result()
                    _count = 1
                    while len(hs_data_list) == 0 and len(ss_data_list) == 0:
                        try:
                            self.refresh_proxies(self._proxies)
                            __page, hs_data_list, ss_data_list, bs_data_list = self.collect_by_page(__page, bizCode)
                        except Exception as e:
                            _count += 1
                            if _count > 10:
                                raise f"采集{_page}页数据失败!"
                            time.sleep(10*(_count-1)+1)
                    logger.info(f" end target_page = {__page}/{self.total_page}, hs_data_list_size: {len(hs_data_list)}, ss_data_list_size: {len(ss_data_list)}, bs_data_list_size: {len(bs_data_list)}")
                    if len(hs_data_list) > 0:
                        self.tmp_df = pd.concat([self.tmp_df, pd.DataFrame(hs_data_list)])
                    if len(ss_data_list) > 0:
                        self.tmp_df = pd.concat([self.tmp_df, pd.DataFrame(ss_data_list)])
                    if len(bs_data_list) > 0:
                        self.tmp_df = pd.concat([self.tmp_df, pd.DataFrame(bs_data_list)])
            self.collect_num = self.tmp_df.index.size
            self.data_text = self.tmp_df.to_csv(index=False)

    def collect_by_page(self, target_page, bizCode):
        retry_count = 5
        params = {'date': self.search_date.strftime('%Y-%m-%d'), 'hsPage': target_page, 'hsPageSize': self.page_size, 'ssPage': target_page,
                  'ssPageSize': self.page_size, 'bsPage': target_page, 'bsPageSize': self.page_size}
        logger.info(f" start target_page = {target_page}/{self.total_page}, params: {params}")
        while retry_count:
            try:
                if retry_count < 5:
                    self.refresh_proxies(_proxies)
                _proxies = self._proxies
                response = requests.post(url=self.url, params=params, proxies=_proxies, timeout=60)
                if response is None or response.status_code != 200:
                    raise Exception(f'{self.data_source}数据采集任务请求响应获取异常,已获取代理ip为:{self._proxies}，请求url为:{self.url},请求参数为:{params}')
                text = json.loads(response.text)
                hs_data_list = text['result'][f'{bizCode}Hs']
                ss_data_list = text['result'][f'{bizCode}Ss']
                bs_data_list = text['result'][f'{bizCode}Bs']
                return target_page, hs_data_list, ss_data_list, bs_data_list
            except Exception as e:
                retry_count -= 1
                time.sleep(5)
        return target_page, [], [], []


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
