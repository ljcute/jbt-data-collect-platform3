#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 13:19
# 长江证券
import os
import sys
import traceback

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler, get_proxies, argv_param_invoke
from utils.deal_date import ComplexEncoder
import json
import time
from constants import *
from utils.logs_utils import logger
import datetime


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '长江证券'
        self.url = 'https://www.95579.com/servlet/json'

    def rzrq_underlying_securities_collect(self):
        params = {"funcNo": "902122", "i_page": 1, "i_perpage": 10000}  # 默认查询当天
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['results'][0]['total_rows'])
        self.tmp_df = pd.DataFrame(text['results'])
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)

    def guaranty_securities_collect(self):
        params = {"funcNo": "902124", "i_page": 1, "i_perpage": 10000}  # 默认查询当天
        response = self.get_response(self.url, 0, get_headers(), params)
        text = json.loads(response.text)
        self.total_num = int(text['results'][0]['total_rows'])
        self.tmp_df = pd.DataFrame(text['results'])
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
