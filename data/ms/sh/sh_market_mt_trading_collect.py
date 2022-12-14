#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/12 17:05
# @Site    :
# @Software: PyCharm
# 上海交易所-融资融券交易汇总及详细数据
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

from data.ms.basehandler import BaseHandler, argv_param_invoke
from constants import get_headers

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sh_balance.xls')

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, "上交所融资融券{}.xls".format(datetime.date.today()))


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '上海交易所'
        self.collect_num_check = False

    def trading_amount_collect(self):
        trade_date = self.get_trade_date()
        self.biz_dt = trade_date
        download_excel_url = "http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk20220623.xls"
        replace_str = 'rzrqjygk' + str(trade_date).format("'%Y%m%d'").replace('-', '') + '.xls'
        download_excel_url = download_excel_url.replace(download_excel_url.split('/')[-1], replace_str)
        self.url = download_excel_url
        response = self.get_response(self.url, 0, get_headers())
        download_excel(response)

        warnings.filterwarnings('ignore')
        self.tmp_df = pd.read_excel(response.content, header=0, sheet_name='汇总信息')
        if not self.tmp_df.empty:
            self.total_num = self.tmp_df.index.size
            self.collect_num = self.total_num
            self.data_text = self.tmp_df.to_csv(index=False)
        else:
            self.data_status = 3

    def trading_items_collect(self):
        trade_date = self.get_trade_date()
        download_excel_url = "http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk20220623.xls"
        replace_str = 'rzrqjygk' + str(trade_date).format("'%Y%m%d'").replace('-', '') + '.xls'
        download_excel_url = download_excel_url.replace(download_excel_url.split('/')[-1], replace_str)
        self.url = download_excel_url
        response = self.get_response(self.url, 0)
        download_excel(response)
        warnings.filterwarnings('ignore')
        self.tmp_df = pd.read_excel(response.content, header=0, sheet_name='明细信息')
        if not self.tmp_df.empty:
            self.total_num = self.tmp_df.index.size
            self.collect_num = self.total_num
            self.data_text = self.tmp_df.to_csv(index=False)
        else:
            self.data_status = 3

    def get_trade_date(self):
        try:
            driver = self.get_driver()
            url = 'http://www.sse.com.cn/market/othersdata/margin/detail/'
            driver.get(url)
            time.sleep(3)
            trade_date = driver.find_elements(By.XPATH,
                                              '/html/body/div[9]/div/div[2]/div/div[1]/div[1]/div[1]/table/tbody/tr/td[1]')[
                0].text
            return trade_date
        except Exception as e:
            raise Exception(e)


def download_excel(response, query_date=None):
    if response.status_code == 200:
        try:
            with open(excel_file_path, 'wb') as file:
                file.write(response.content)  # 写excel到当前目录
            with open(save_excel_file_path, 'wb') as file:
                file.write(response.content)  # 写excel到当前目录
        except Exception as es:
            raise Exception(es)


if __name__ == "__main__":
    argv_param_invoke(CollectHandler(), (0, 1), sys.argv)
