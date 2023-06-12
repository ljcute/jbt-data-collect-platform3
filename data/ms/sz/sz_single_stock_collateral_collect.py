#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yexy
# @Time    : 2023/6/9 11:05
# @Site    :
# @Software: PyCharm
# 深圳交易所-单一股票担保物比例信息 https://www.bse.cn/disclosure/rzrq_dygpdbwbl.html
import sys
import time
import datetime
import os
import warnings
import pandas as pd
import random

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from configparser import ConfigParser
from selenium.webdriver.common.by import By
from data.ms.basehandler import BaseHandler, argv_param_invoke, logger, USER_AGENTS, random_double

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sh_balance.xls')

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '深圳交易所'
        self.collect_num_check = False

    def single_stock_collateral_collect(self):
        logger.info(f"开始爬取深圳交易所-单一股票担保物比例信息:{datetime.datetime.now()}")
        trade_date = self.get_trade_date()
        self.biz_dt = trade_date
        headers = {
            'User-Agent': random.choice(USER_AGENTS)
        }
        self.url = 'http://www.szse.cn/api/report/ShowReport'
        params = {
            'SHOWTYPE': 'xlsx',
            'CATALOGID': 'rzrqdbwbl',
            'TABKEY': 'tab1',
            # 查历史可以传日期
            'txtDate': trade_date,
            'random': random_double()
        }
        if isinstance(param_dt, str):
            logger.info(f'param_dt:{param_dt, type(param_dt)}')
            logger.info(f'进入查询深交所历史单一股票担保物比例信息!')
            params['txtDate'] = param_dt
        response = self.get_response(self.url, 0, headers, params)
        warnings.filterwarnings('ignore')
        self.tmp_df = pd.read_excel(response.content, header=0)
        if not self.tmp_df.empty:
            self.total_num = self.tmp_df.index.size
            self.collect_num = self.total_num
            self.data_text = self.tmp_df.to_csv(index=False)
        else:
            self.data_status = 3


    def get_trade_date(self):
        try:
            driver = self.get_driver()
            url = 'http://www.szse.cn/disclosure/margin/ratio/index.html'
            driver.get(url)
            time.sleep(3)
            trade_date = driver.find_elements(By.XPATH, '/html/body/div[5]/div/div[2]/div/div/div[4]/div/div[1]/div[1]/span[2]')[0].text
            return trade_date
        except Exception as e:
            raise Exception(e)


if __name__ == "__main__":
    argv = sys.argv
    param_dt = None
    if len(argv) == 3:
        param_dt = argv[2]
    argv_param_invoke(CollectHandler(), (7,), sys.argv)
    #CollectHandler().collect_data(7)