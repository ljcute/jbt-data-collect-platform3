#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/20 10:38

import os
import sys
import json
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, random_page_size, argv_param_invoke


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '国信证券'

    def _securities_collect(self):
        params = {"pageSize": random_page_size(), "pageNo": 1}
        response = self.get_response(self.url, 1, None, None, params)
        text = json.loads(response.text)
        self.total_num = int(text['data'][0]['count'])
        self.tmp_df = pd.DataFrame(text['data'])
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)

    def rz_underlying_securities_collect(self):
        # type=0表示融资
        self.url = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqbdzq/1.0?type=0'
        self._securities_collect()

    def rq_underlying_securities_collect(self):
        # type=0表示融资
        self.url = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqbdzq/1.0?type=1'
        self._securities_collect()

    def guaranty_securities_collect(self):
        self.url = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqkcdbzjzq/1.0'
        self._securities_collect()


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 4, 5), sys.argv)
