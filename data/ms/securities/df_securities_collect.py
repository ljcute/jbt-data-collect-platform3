#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2023/02/23 15:16
import json
import os
import sys
import time
import pandas as pd
from selenium.webdriver.common.by import By


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, logger, argv_param_invoke
from constants import get_headers


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '东方证券'
        self.total_page = 0
        self.page_no = 1


    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.dfzq.com.cn/servlet/json'
        self._securities_collect('bd')

    def guaranty_securities_collect(self):
        self.url = 'https://www.dfzq.com.cn/servlet/json'
        self._securities_collect('db')

    def _securities_collect(self, biz_type):
        data = {
            "funcNo": 501003,
            "pageNum": 1,
            "pageSize": 10000,
            "securitiescode":None,
            "securitiestype":None,
            "search_date": self.search_date
        }
        if biz_type == 'db':
            data['funcNo'] = 501003
        elif biz_type == 'bd':
            data['funcNo'] = 501002
        response = self.get_response(self.url, 0, get_headers(), data)
        text = json.loads(response.text)
        self.total_num = text['results'][0]['totalRows']
        self.tmp_df = pd.DataFrame(text['results'][0]['data'])
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
