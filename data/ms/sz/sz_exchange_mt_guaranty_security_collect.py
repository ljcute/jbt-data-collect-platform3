#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/27 09:33
# 深圳交易所-融资融券可充抵保证金证券

import os
import sys
import time
import traceback
from configparser import ConfigParser


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder
from utils.remove_file import remove_file, random_double


from utils.exceptions_utils import ProxyTimeOutEx
import json
import xlrd2
from constants import USER_AGENTS
import random
import os
import datetime
import re
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sz_exchange_mt_guaranty_security.xlsx')

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, "深交所担保券{}.xlsx".format(datetime.date.today()))

regrex_pattern = re.compile(r"[(](.*?)[)]", re.S)  # 最小匹配,提取括号内容
regrex_pattern2 = re.compile(r"[(](.*)[)]", re.S)  # 贪婪匹配


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '深圳交易所'

    def guaranty_securities_collect(self):
        actual_date = datetime.date.today() if self.search_date is None else self.search_date
        self.url = 'http://www.szse.cn/api/report/ShowReport'
        headers = {
            'User-Agent': random.choice(USER_AGENTS)
        }
        params = {
            'SHOWTYPE': 'xlsx',
            'CATALOGID': '1835_xxpl',
            'TABKEY': 'tab1',
            # 查历史可以传日期
            'txtDate': actual_date,
            'random': random_double()
        }
        response = self.get_response(self.url, 0, headers, params)
        self.download_excel(response)
        excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
        self.data_list, total_row = self.handle_excel(excel_file, actual_date)
        self.total_num = total_row - 1
        self.collect_num = len(self.data_list)

    def download_excel(self, response):
        try:
            with open(excel_file_path, 'wb') as file:
                file.write(response.content)
            with open(save_excel_file_path, 'wb') as file:
                file.write(response.content)
        except Exception as es:
            raise Exception(es)

    def handle_excel(self, excel_file, actual_date):
        logger.info("开始处理excel")
        sheet_0 = excel_file.sheet_by_index(0)
        total_row = sheet_0.nrows
        try:
            if total_row > 1:
                data_list = []
                for i in range(1, total_row):
                    row = sheet_0.row(i)
                    if row is None:
                        break

                    zqdm = str(row[0].value)  # 证券代码
                    zqjc = str(row[1].value)  # 证券简称
                    data_list.append((zqdm, zqjc))

                logger.info(f'已采集数据条数：{total_row - 1}')
                return data_list, total_row
            else:
                logger.info("深交所该日无数据:txt_date:{}".format(actual_date))

        except Exception as es:
            raise Exception(es)


if __name__ == '__main__':
    collector = CollectHandler()
    if len(sys.argv) > 1:
        collector.collect_data(eval(sys.argv[1]))
    else:
        logger.error(f'business_type为必传参数')
