#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 10:19
# 招商证券 --interface
import os
import sys
import json
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, get_headers, random_page_size


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '招商证券'

    def rz_underlying_securities_collect(self):
        self.url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstock'
        params = {"pageSize": random_page_size(), "pageNum": 1, "rqbdflag": 1}  # rqbdflag = 1融资
        self._securities_collect(params)

    def rq_underlying_securities_collect(self):
        self.url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstock'
        params = {"pageSize": random_page_size(), "pageNum": 1, "rqbdflag": 2}  # rqbdflag = 1融资,2融券
        self._securities_collect(params)

    def guaranty_securities_collect(self):
        self.url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstockdiscount'
        params = {"pageSize": random_page_size(), "pageNum": 1}
        self._securities_collect(params)

    def _securities_collect(self, params):
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['body']['totalNum'])
        self.tmp_df = pd.DataFrame(text['body']['stocks'])
        self.data_text = self.tmp_df.to_string()
        self.collect_num = self.tmp_df.index.size


if __name__ == '__main__':
    CollectHandler().argv_param_invoke((2, 4, 5), sys.argv)
