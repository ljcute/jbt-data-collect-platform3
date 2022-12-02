#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/9/19 10:41
# @Site    : 
# @File    : zjcf_securities_collect.py
# @Software: PyCharm
import os
import sys
import json
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler, get_headers, random_page_size, argv_param_invoke


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '中金财富'

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.ciccwm.com/api2/web2016/cicc/rzrq/getTargetStockInfo?'
        self._securities_collect()

    def guaranty_securities_collect(self):
        self.url = 'https://www.ciccwm.com/api2/web2016/cicc/rzrq/getRzrqGuaranteeInfo?'
        self._securities_collect()

    def _securities_collect(self):
        params = {'pageNum': 1, 'pageSize': random_page_size()}
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['data']['total'])
        self.tmp_df = pd.DataFrame(text['data']['list'])
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
