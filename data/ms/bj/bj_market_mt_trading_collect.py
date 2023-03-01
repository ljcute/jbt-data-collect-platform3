#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/2/15 9:29
# @Site    : 
# @File    : bj_market_mt_trading_collect.py
# @Software: PyCharm
# 北京交易所-融资融券交易汇总及详细数据
import json
import logging
import sys
import time
import datetime
import os
import warnings
import pandas as pd
from configparser import ConfigParser

from selenium.webdriver.common.by import By

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler, argv_param_invoke, logger
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
        stamp = datetime.datetime.now().timestamp()
        if isinstance(param_dt, str):
            logger.info(f'param_dt:{param_dt, type(param_dt)}')
            logger.info(f'进入查询北交所历史交易数据分支!')
            self.url = f'https://www.bse.cn/rzrqjyyexxController/summaryInfoResult.do?callback=jQuery331_{stamp}&transDate={param_dt}&page=0&_={stamp}'
        else:
            self.url = f'https://www.bse.cn/rzrqjyyexxController/summaryInfoResult.do?callback=jQuery331_{stamp}&transDate=&page=0&_={stamp}'

        response = self.get_response(self.url, 0, get_headers())
        temp_text = response.text
        text = eval(temp_text[temp_text.index('('):])
        data_list = text[0]
        self.tmp_df = pd.DataFrame(data_list)
        self.tmp_df['业务日期'] = text[1]
        if not self.tmp_df.empty:
            self.total_num = self.tmp_df.index.size
            self.collect_num = self.total_num
            self.data_text = self.tmp_df.to_csv(index=False)
        else:
            self.data_status = 3
        # self.url = f"https://www.bse.cn/rzrqjyyexxController/exportSummary.do?transDate={self.trade_date}"
        # response = self.get_response(self.url, 0, get_headers())
        # warnings.filterwarnings('ignore')
        # df = pd.read_excel(response.content, header=0)
        # if not df.empty:
        #     # 日期处理
        #     dt_str = df.values[-1][0]
        #     if '日期' in dt_str:
        #         dt = dt_str.replace('日期', '').replace('：', '')
        #         df.drop([len(df) - 1], inplace=True)
        #         df['业务日期'] = dt
        #     self.total_num = df.index.size
        #     self.collect_num = self.total_num
        #     self.data_text = df.to_csv(index=False)

    def trading_items_collect(self):
        page = 0
        stamp = datetime.datetime.now().timestamp()
        self.url = f'https://www.bse.cn/rzrqjyyexxController/detailInfoResult.do?callback=jQuery331_{stamp}'
        if isinstance(param_dt, str):
            logger.info(f'param_dt:{param_dt, type(param_dt)}')
            logger.info(f'进入查询北交所历史交易数据分支!')
            data = {
                "transDate": param_dt,
                "page": page,
                "sortfield": None,
                "sorttype": None
            }
        else:
            data = {
                "transDate": None,
                "page": page,
                "sortfield": None,
                "sorttype": None
            }
        response = self.get_response(self.url, 1, get_headers(), data=data)
        temp_text = response.text
        s = temp_text[temp_text.index('['):]
        s = s[:-1]
        text = eval(s.replace('true', 'True').replace('false', 'False').replace('null', 'None'))
        total_pages = text[0][0]['totalPages']
        self.total_num = text[0][0]['totalElements']
        for i in range(0, total_pages):
            if isinstance(param_dt, str):
                logger.info(f'param_dt:{param_dt, type(param_dt)}')
                logger.info(f'进入查询北交所历史交易数据分支!')
                data_ = {
                    "transDate": param_dt,
                    "page": i,
                    "sortfield": None,
                    "sorttype": None
                }
            else:
                data_ = {
                    "transDate": None,
                    "page": i,
                    "sortfield": None,
                    "sorttype": None
                }
            response_ = self.get_response(self.url, 1, get_headers(), data=data_)
            temp_text_ = response_.text
            s_ = temp_text_[temp_text_.index('['):]
            s_ = s_[:-1]
            text_ = eval(s_.replace('true', 'True').replace('false', 'False').replace('null', 'None'))
            data_list_ = text_[0][0]['content']
            self.tmp_df = pd.concat([self.tmp_df, pd.DataFrame(data_list_)])

        self.tmp_df['业务日期'] = text[1]
        if not self.tmp_df.empty:
            self.collect_num = self.tmp_df.index.size
            self.data_text = self.tmp_df.to_csv(index=False)
        else:
            self.data_status = 3

        # self.url = f"https://www.bse.cn/rzrqjyyexxController/exportDetail.do?transDateDetail={self.trade_date}"
        # response = self.get_response(self.url, 0, get_headers())
        # warnings.filterwarnings('ignore')
        # df = pd.read_excel(response.content, header=0)
        # if not df.empty:
        #     # 日期处理
        #     dt_str = df.values[-1][0]
        #     if '日期' in dt_str:
        #         dt = dt_str.replace('日期', '').replace('：', '')
        #         df.drop([len(df) - 1], inplace=True)
        #         df['业务日期'] = dt
        #     self.total_num = df.index.size
        #     self.collect_num = self.total_num
        #     self.data_text = df.to_csv(index=False)


if __name__ == '__main__':
    argv = sys.argv
    param_dt = None
    if len(argv) == 3:
        param_dt = argv[2]
    argv_param_invoke(CollectHandler(), (0, 1), sys.argv)
