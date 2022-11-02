#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/29 16:47

import os
import sys
import json
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, get_headers


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '中信建投'
        self.url = 'https://www.csc108.com/kyrz/rzrqList.json'

    def guaranty_and_underlying_securities_collect(self):
        params = {'curPage': 1}
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.tmp_df = pd.DataFrame(text['list'])
        self.total_num = int(text['totalCount'])
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)


if __name__ == '__main__':
    CollectHandler().argv_param_invoke((99, ), sys.argv)
