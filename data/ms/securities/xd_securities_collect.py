#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/2/16 16:12
# @Site    : 
# @File    : xd_securities_collect.py
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
from data.ms.basehandler import BaseHandler, logger, random, USER_AGENTS, argv_param_invoke


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '信达证券'
        self.url = 'https://www.cindasc.com/servlet/json'
        self.funcNo = 0
        self.page_size = 10
        self.total_page = 0
        self.init_date = None
        self._proxies = self.get_proxies()
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '90',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': 'Hm_lvt_0e79ee36ae8f6a764ecfe08607f134f4=1676529281; Hm_lpvt_0e79ee36ae8f6a764ecfe08607f134f4=1676535002',
            'Host': 'www.cindasc.com',
            'Origin': 'https://www.cindasc.com',
            'Referer': 'https://www.cindasc.com/osoa/views/xdyw/zqjr/rzrq/zqcx/index.html',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': random.choice(USER_AGENTS),
            'X-Requested-With': 'XMLHttpRequest'
        }

    def rz_underlying_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('rz')

    def rq_underlying_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('rq')

    def guaranty_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('db')

    def _get_params(self, biz_type, target_page):
        params = {"funcNo": self.funcNo, "secu_code": None, 'security_type': None, 'trade_city': None,
                  'kcb_date': None, 'currentPage': target_page, 'numPerPage': 100000}
        if biz_type == 'db':
            params["funcNo"] = 2000102
        elif biz_type == 'rq':
            params["funcNo"] = 2000101
        elif biz_type == 'rz':
            params["funcNo"] = 2000100
        return params

    def _securities_collect(self, biz_type):
        params = self._get_params(biz_type, 1)
        response = self.get_response(self.url, 1, self.headers, None,params)
        text = json.loads(response.text)
        self.total_num = int(text['DataSet1'][0]['total_rows'])
        self.tmp_df = pd.DataFrame(text['DataSet0'])
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 4, 5), sys.argv)
