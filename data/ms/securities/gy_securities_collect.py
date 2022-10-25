#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/20 14:21
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
        self.data_source = '国元证券'
        self.url = 'http://www.gyzq.com.cn/servlet/json'

    def _securities_collect(self, func_no, cxlx=None):
        # cxlx 0融资,1融券
        data = {
            'date': self.search_date.strftime('%Y-%m-%d'),
            'funcNo': func_no
        }
        if cxlx is not None:
            data["cxlx"] = cxlx
        response = self.get_response(self.url, 2, get_headers(), None, data)
        text = json.loads(response.text)
        self.tmp_df = pd.DataFrame(text['results'])
        self.collect_num = self.tmp_df.index.size
        self.total_num = self.collect_num
        self.data_text = self.tmp_df.to_string()

    def rz_underlying_securities_collect(self):
        self._securities_collect(904103, 0)

    def rq_underlying_securities_collect(self):
        self._securities_collect(904103, 1)

    def guaranty_securities_collect(self):
        self._securities_collect(904104)


if __name__ == '__main__':
    CollectHandler().argv_param_invoke((2, 4, 5), sys.argv)
