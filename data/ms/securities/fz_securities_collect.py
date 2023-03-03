#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/3/3 13:32
# @Site    : 
# @File    : fz_securities_collect.py
# @Software: PyCharm
import os
import sys
import json
import time
import math
import requests
import pandas as pd
import concurrent.futures
import urllib3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, get_headers, logger, argv_param_invoke
from constants import *


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '方正证券'
        self.page_size = 10
        self._proxies = self.get_proxies()
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '53',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': 'cookiesession1=0A2CF6C02VGQFPWA7EWUSUNDYAOM3DFE; _site_id_cookie=1; JSESSIONID=7D582C8B441D5E6B592A3C9FE34226BD',
            'Host': 'www.foundersc.com',
            'Origin': 'https://www.foundersc.com',
            'Referer': 'https://www.foundersc.com/creditMarginPublicNewsListNew/index.jhtml',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': random.choice(USER_AGENTS),
            'X-Requested-With': 'XMLHttpRequest'
        }

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.foundersc.com/infoMargin/getFzRzrqRatioAndSecuritiesNewList.jspx'
        self.data = {
            "pageNo": 1,
            "pageSize": 10000,
            "stockCode": None,
            "stockName": None,
            "tradeDate": str(self.search_date).split(' ')[0]
        }
        self._securities_collect()

    def guaranty_securities_collect(self):
        self.url = 'https://www.foundersc.com/infoMargin/getFzRzrqSecuritiesList.jspx'
        self.data = {
            "pageNo": 1,
            "pageSize": 10000,
            "stockCode": None,
            "stockName": None,
            "tradeDate": str(self.search_date).split(' ')[0]
        }
        self._securities_collect()

    def _securities_collect(self):
        logger.info(f'data:{self.data}')
        urllib3.disable_warnings()
        response = requests.post(url=self.url, data=self.data, proxies=self._proxies, headers=self.headers, timeout=30,
                                 verify=False)
        text = json.loads(response.text)
        self.tmp_df = pd.DataFrame(text['body']['list'])
        self.collect_num = self.total_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
