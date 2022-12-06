#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/29 16:47
import math
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
        self.data_source = '中信建投'
        self.url = 'https://www.csc108.com/marginTrading'
        self.url = 'https://wwwxchw.csc108.com/server/siteindex/margin/getRzrqList?stkcode=&market=&type=&fundctrlflag=&stkctrlflag=&current=1&size=10000'

    def guaranty_and_underlying_securities_collect(self):
        pages = math.inf
        size = 10000
        url = self.url
        while pages:
            response = self.get_response(url, 0, get_headers())
            data = json.loads(response.text)['data']
            if pages == math.inf:
                self.total_num = int(data['total'])
                pages = int(data['pages'])
                size = int(data['size'])
            pages -= 1
            url = self.url.replace('current=1', f"current={data['pages'] -pages + 1}").replace('size=10000', f'size={size}')
            self.tmp_df = pd.concat([self.tmp_df, pd.DataFrame(data['records'])])
            self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (99, ), sys.argv)
