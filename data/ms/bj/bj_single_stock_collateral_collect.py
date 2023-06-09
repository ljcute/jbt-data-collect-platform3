#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yexy
# @Time    : 2023/6/08 16:00
# @Site    :
# @File    : bj_single_stock_collateral_collect.py
# @Software: PyCharm
# 北京交易所-单一股票担保物比例信息
import sys
import datetime
import os
import pandas as pd
from configparser import ConfigParser

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

    def single_stock_collateral_collect(self):
        page = 0
        self.url = f'https://www.bse.cn/dygpdbwblController/infoResult.do?callback=jQuery331_1686213639369'
        if isinstance(param_dt, str):
            logger.info(f'param_dt:{param_dt, type(param_dt)}')
            logger.info(f'进入查询北交所历史交易数据分支!')
            data = {
                "transDate": param_dt,
                "page": page,
                "zqdm": None,
                "sortfield":None,
                "sorttype": None
            }
        else:
            data = {
                "transDate": None,
                "page": page,
                "zqdm": None,
                "sortfield":None,
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

if __name__ == '__main__':
    argv = sys.argv
    param_dt = None
    if len(argv) == 3:
        param_dt = argv[2]
    argv_param_invoke(CollectHandler(), (6), sys.argv)
    #CollectHandler().collect_data(6)
