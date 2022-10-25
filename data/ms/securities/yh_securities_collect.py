#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/28 13:34

import os
import sys
import time
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '中国银河'
        self.url = 'http://www.chinastock.com.cn/newsite/cgs-services/stockFinance/businessAnnc.html?type=marginList'

    def guaranty_and_underlying_securities_collect(self):
        try:
            driver = self.get_driver()
            driver.get(self.url)
            time.sleep(5)
            df_list = pd.read_html(str(driver.page_source))
            for df_str in df_list:
                df = pd.DataFrame(df_str)
                if ('证券代码', '证券简称', '融券保证金比例') == tuple(df.columns):
                    df['type'] = "rq"
                    df.rename(columns={r'融券保证金比例': 'rate'}, inplace=True)
                elif ('证券代码', '证券简称', '融资保证金比例') == tuple(df.columns):
                    df['type'] = "rz"
                    df.rename(columns={r'融资保证金比例': 'rate'}, inplace=True)
                elif ('证券代码', '证券简称', '折算率') == tuple(df.columns):
                    df['type'] = "db"
                    df.rename(columns={r'折算率': 'rate'}, inplace=True)
                else:
                    continue
                self.tmp_df = pd.concat([self.tmp_df, df])
            self.collect_num = self.tmp_df.index.size
            self.total_num = self.collect_num
            self.data_text = self.tmp_df.to_string()
        finally:
            driver.quit()


if __name__ == '__main__':
    CollectHandler().argv_param_invoke((99, ), sys.argv)
