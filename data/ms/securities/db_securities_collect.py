#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/2/20 16:01
# @Site    : 
# @File    : db_securities_collect.py
# @Software: PyCharm
import json
import os
import sys
import pandas as pd


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, get_headers, logger, argv_param_invoke


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '东北证券'
        self.init_date = None
        self.url = ''
        self._proxies = self.get_proxies()
        self.total_page = 0
        self.page_no = 1

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.nesc.cn/dbzq/jrfw/loadNr.jsp'
        self._securities_collect('bd')

    def guaranty_securities_collect(self):
        self.url = 'https://www.nesc.cn/dbzq/jrfw/loadNr.jsp'
        self._securities_collect('db')

    def _securities_collect(self, biz_type):
        data = {
            'act': '',
            'pageIndex': self.page_no,
            'keyword': None
        }
        if biz_type == 'bd':
            data['act'] = 'bd'
        elif biz_type == 'db':
            data['act'] = 'kcd'
        response = self.get_response(self.url, 0, get_headers(), data)
        text = json.loads(response.text)
        result_list = text['result']
        if result_list:
            self.total_num = text['all']
            self.total_page = text['pageCount']
            self.tmp_df = pd.concat([self.tmp_df, pd.DataFrame(result_list)])
            for i in range(2, self.total_page + 1):
                logger.info(f'第{i}页')
                data['pageIndex'] = i
                response = self.get_response(self.url, 0, get_headers(), data)
                text = json.loads(response.text)
                result_list = text['result']
                temp_df = pd.DataFrame(result_list)
                self.tmp_df = pd.concat([self.tmp_df, temp_df])

            self.collect_num = self.tmp_df.index.size
            self.data_text = self.tmp_df.to_csv(index=False)




if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
