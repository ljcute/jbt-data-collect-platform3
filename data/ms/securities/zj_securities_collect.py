#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/12/20 15:02
# @Site    :
# @File    : zj_securities_collect.py
# @Software: PyCharm
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
        self.data_source = '中金公司'
        self.url = 'https://www.cicc.com/ciccdata/reportservice/showreportdata.do?reportId=MARGINGROUPINFO&pageIndex=0&pageSize=10000'

    def guaranty_and_underlying_securities_collect(self):
        response = self.get_response(self.url, 0, get_headers())
        data = json.loads(response.text)['data']
        self.total_num = int(len(data))
        self.tmp_df = pd.DataFrame(data)
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (99, ), sys.argv)