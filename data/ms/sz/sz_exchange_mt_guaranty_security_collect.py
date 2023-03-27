#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/27 09:33
# 深圳交易所-融资融券可充抵保证金证券

import os
import sys
import random
import warnings
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, random_double, USER_AGENTS, argv_param_invoke


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '深圳交易所'
        self.collect_num_check = False

    def guaranty_securities_collect(self):
        self.url = 'http://www.szse.cn/api/report/ShowReport'
        headers = {
            'User-Agent': random.choice(USER_AGENTS)
        }
        params = {
            'SHOWTYPE': 'xlsx',
            'CATALOGID': '1835_xxpl_snapshot',
            'TABKEY': 'tab1',
            # 查历史可以传日期
            'txtDate': self.search_date.strftime('%Y-%m-%d'),
            'random': random_double()
        }
        response = self.get_response(self.url, 0, headers, params)
        warnings.filterwarnings('ignore')
        self.tmp_df = pd.read_excel(response.content, header=0)
        if not self.tmp_df.empty:
            self.total_num = self.tmp_df.index.size
            self.collect_num = self.total_num
            self.data_text = self.tmp_df.to_csv(index=False)
        else:
            self.data_status = 3


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2,), sys.argv)
