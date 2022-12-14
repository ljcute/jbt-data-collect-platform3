#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/24 13:33
# 深圳交易所-市场融资融券交易总量/市场融资融券交易明细

import os
import sys
import time
import warnings
from configparser import ConfigParser

import pandas as pd
from selenium.webdriver.common.by import By

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler, random_double, argv_param_invoke
from utils.remove_file import remove_file, random_double
import datetime
from constants import USER_AGENTS
import random
import os
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sz_balance.xlsx')
excel_file_path_anthoer = os.path.join(base_dir, 'sz_balance_total.xlsx')

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, "深交所融资融券{}.xlsx".format(datetime.date.today()))
save_excel_file_path_total = os.path.join(paths, "深交所融资融券交易总量{}.xlsx".format(datetime.date.today()))


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '深圳交易所'
        self.collect_num_check = False

    def trading_amount_collect(self):
        trade_date = self.get_trade_date()
        self.biz_dt = trade_date
        self.url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1837_xxpl&TABKEY=tab1'
        headers = {
            'User-Agent': random.choice(USER_AGENTS)
        }
        params = {
            'txtDate': trade_date,  # 查历史可以传日期
            'random': random_double(),
        }
        response = self.get_response(self.url, 0, headers, params)
        download_excel(response)
        warnings.filterwarnings('ignore')
        self.tmp_df = pd.read_excel(response.content, header=0)
        if not self.tmp_df.empty:
            self.total_num = self.tmp_df.index.size
            self.collect_num = self.total_num
            self.data_text = self.tmp_df.to_csv(index=False)
        else:
            self.data_status = 3

    def trading_items_collect(self):
        trade_date = self.get_trade_date()
        self.biz_dt = trade_date
        self.url = "https://www.szse.cn/api/report/ShowReport"
        headers = {
            'User-Agent': random.choice(USER_AGENTS)
        }
        params = {
            'SHOWTYPE': 'xlsx',
            'CATALOGID': '1837_xxpl',
            'txtDate': trade_date,  # 查历史可以传日期
            'random': random_double(),
            'TABKEY': 'tab2'
        }
        response = self.get_response(self.url, 0, headers, params)
        download_excel(response)
        warnings.filterwarnings('ignore')
        self.tmp_df = pd.read_excel(response.content, header=0)
        if not self.tmp_df.empty:
            self.total_num = self.tmp_df.index.size
            self.collect_num = self.total_num
            self.data_text = self.tmp_df.to_csv(index=False)
        else:
            self.data_status = 3

    def get_trade_date(self):
        url = 'http://www.szse.cn/disclosure/margin/margin/index.html'
        try:
            logger.info(f'开始获取深圳交易所最新交易日日期')
            driver = super().get_driver()
            driver.get(url)
            time.sleep(3)
            trade_date = \
                driver.find_elements(By.XPATH,
                                     '/html/body/div[5]/div/div[2]/div/div/div[4]/div[1]/div[1]/div[1]/span[2]')[
                    0].text
            logger.info(f'深圳交易所最新交易日日期为{trade_date}')
            return trade_date

        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            raise Exception(f'获取深圳交易所最新交易日日期异常，请求url为：{url}，具体异常信息为：{e}')


def download_excel(response, query_date=None):
    if response.status_code == 200:
        try:
            with open(excel_file_path, 'wb') as file:
                file.write(response.content)  # 写excel到当前目录
            with open(save_excel_file_path, 'wb') as file:
                file.write(response.content)  # 写excel到当前目录
        except Exception as es:
            raise Exception(es)
    else:
        logger.info("深交所该日无数据:txt_date:{}".format(query_date))


if __name__ == "__main__":
    argv_param_invoke(CollectHandler(), (0, 1), sys.argv)
