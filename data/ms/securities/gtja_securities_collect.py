#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/19 13:14
# 国泰君安
import os
import sys
import json
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, get_headers, argv_param_invoke


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '国泰君安'

    def guaranty_and_underlying_securities_collect(self):
        self.url = f'https://www.gtja.com/cos/rest/margin/path/fuzzy.json?stamp={self.search_date}'
        response = self.get_response(self.url, 0, get_headers())
        text = json.loads(response.text)
        self.tmp_df = pd.concat([pd.DataFrame(text["offset"]), pd.DataFrame(text["security"]), pd.DataFrame(text["finance"])])
        self.collect_num = self.tmp_df.index.size
        self.total_num = self.collect_num
        self.data_text = self.tmp_df.to_csv(index=False)


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (99, ), sys.argv)