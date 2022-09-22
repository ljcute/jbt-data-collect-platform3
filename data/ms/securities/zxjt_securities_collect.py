#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/29 16:47
# 中信建投

import os
import sys
import traceback
from configparser import ConfigParser

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler, get_proxies
from utils.deal_date import ComplexEncoder

import json
import os
from constants import *
from utils.logs_utils import logger
import datetime

broker_id = 10006
guaranty_file_path = './' + str(broker_id) + 'guaranty.xls'
target_file_path = './' + str(broker_id) + 'target.xls'
all_file_path = './' + str(broker_id) + 'all.xls'

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, "中信建投证券三种数据整合{}.xls".format(datetime.date.today()))


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '中信建投'
        self.url = 'https://www.csc108.com/kyrz/rzrqList.json'

    def guaranty_and_underlying_securities_collect(self):
        params = {'curPage': 1}
        self.data_list = []
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['totalCount'])
        data = text['list']
        for i in data:
            # sec_code = i['stkCode']
            # market = '深A' if i['market'] == '0' else '沪A'
            # sec_name = i['stkName']
            # bzj_type = i['type']
            # if i['type'] == '2':
            #     bzj_type = '2'
            # elif i['type'] is None:
            #     bzj_type = '1'
            # elif i['type'] == '3':
            #     bzj_type = '3'
            # bzj_rate = '-' if i['pledgerate'] is None else i['pledgerate']
            # rz_rate = '-' if i['marginratefund'] is None else i['marginratefund']
            # rq_rate = '-' if i['marginratestk'] is None else i['marginratestk']
            # rz_flag = '-' if i['fundctrlflag'] is None else i['fundctrlflag']
            # rq_flag = '-' if i['stkctrlflag'] is None else i['stkctrlflag']
            self.data_list.append(i)
            self.collect_num = int(len(self.data_list))


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(99)
    collector.collect_data(eval(sys.argv[1]))
