#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/2/15 9:29
# @Site    : 
# @File    : bj_market_mt_trading_collect.py
# @Software: PyCharm
# 北京交易所-融资融券交易汇总及详细数据
import sys
import time
import datetime
import os
import warnings
import pandas as pd
from configparser import ConfigParser


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler, argv_param_invoke
from constants import get_headers
from utils.deal_date import last_work_day


base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'bj_balance.xls')

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, "北交所融资融券{}.xls".format(datetime.date.today()))


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '北京交易所'
        self.collect_num_check = False
        self.trade_date = last_work_day(self.search_date)

    def trading_amount_collect(self):
        self.url = f"https://www.bse.cn/rzrqjyyexxController/exportSummary.do?transDate={self.trade_date}"
        response = self.get_response(self.url, 0, get_headers())
        warnings.filterwarnings('ignore')
        df = pd.read_excel(response.content, header=0)
        if not df.empty:
            # 日期处理
            dt_str = df.values[-1][0]
            if '日期' in dt_str:
                dt = dt_str.replace('日期', '').replace('：', '')
                df.drop([len(df) - 1], inplace=True)
                df['业务日期'] = dt
            self.total_num = df.index.size
            self.collect_num = self.total_num
            self.data_text = df.to_csv(index=False)

    def trading_items_collect(self):
        self.url = f"https://www.bse.cn/rzrqjyyexxController/exportDetail.do?transDateDetail={self.trade_date}"
        response = self.get_response(self.url, 0, get_headers())
        warnings.filterwarnings('ignore')
        df = pd.read_excel(response.content, header=0)
        if not df.empty:
            # 日期处理
            dt_str = df.values[-1][0]
            if '日期' in dt_str:
                dt = dt_str.replace('日期', '').replace('：', '')
                df.drop([len(df) - 1], inplace=True)
                df['业务日期'] = dt
            self.total_num = df.index.size
            self.collect_num = self.total_num
            self.data_text = df.to_csv(index=False)


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (0, 1), sys.argv)
