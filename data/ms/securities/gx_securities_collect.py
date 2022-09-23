#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/20 10:38
# 国信证券

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils import remove_file
from data.ms.basehandler import BaseHandler, get_proxies
import json
import datetime


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '国信证券'

    def rz_underlying_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        page = 1
        page_size = remove_file.random_page_size()
        # params = {"pageSize": page_size, "pageNo": page, "rq": search_date}  # type=0表示融资
        params = {"pageSize": page_size, "pageNo": page}  # type=0表示融资
        self.url = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqbdzq/1.0?type=0'
        self.data_list = []
        self.title_list = ['market', 'stock_code', 'stock_name', 'rz_rate']
        # response = requests.post(self.url, params, self.get_proxies())
        response = self.get_response(self.url, 1, None, None, params)
        text = json.loads(response.text)
        all_data_list = text['data']
        self.total_num = int(text['data'][0]['count'])
        if all_data_list:
            for i in all_data_list:
                # if str(i['sc']) == "1":
                #     market = '深圳'
                # elif str(i['sc']) == "0":
                #     market = '上海'
                # else:
                #     market = '北京'
                # stock_code = i['zqdm']
                # stock_name = i['zqmc']
                # rate = i['rzbzjbl']
                self.data_list.append(i)
                self.collect_num = int(len(self.data_list))

    def rq_underlying_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        page = 1
        page_size = remove_file.random_page_size()
        # params = {"pageSize": page_size, "pageNo": page, "rq": search_date}  # type=0表示融资
        params = {"pageSize": page_size, "pageNo": page}  # type=0表示融资
        self.url = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqbdzq/1.0?type=1'
        self.data_list = []
        self.title_list = ['market', 'stock_code', 'stock_name', 'rq_rate']
        # response = requests.post(self.url, params, self.get_proxies())
        response = self.get_response(self.url, 1, None, None, params)
        text = json.loads(response.text)
        all_data_list = text['data']
        self.total_num = int(text['data'][0]['count'])
        if all_data_list:
            for i in all_data_list:
                # if str(i['sc']) == "1":
                #     market = '深圳'
                # elif str(i['sc']) == "0":
                #     market = '上海'
                # else:
                #     market = '北京'
                # stock_code = i['zqdm']
                # stock_name = i['zqmc']
                # rate = i['rzbzjbl']
                self.data_list.append(i)
                self.collect_num = int(len(self.data_list))

    def guaranty_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else None
        search_date = str(search_date).replace('-', '').replace('/', '') if search_date is not None else None
        page = 1
        page_size = remove_file.random_page_size()
        # params = {"pageSize": page_size, "pageNo": page, "rq": search_date}  # type=0表示融资
        params = {"pageSize": page_size, "pageNo": page}  # type=0表示融资
        self.url = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqkcdbzjzq/1.0'
        self.data_list = []
        self.title_list = ['stock_code', 'stock_name', 'zsl']
        # response = requests.post(self.url, params, self.get_proxies())
        response = self.get_response(self.url, 1, None, None, params)
        text = json.loads(response.text)
        all_data_list = text['data']
        self.total_num = int(text['data'][0]['count'])
        if all_data_list:
            for i in all_data_list:
                # stock_code = i['zqdm']
                # stock_name = i['zqmc']
                # zsl = i['zsl']
                self.data_list.append(i)
                self.collect_num = int(len(self.data_list))


if __name__ == '__main__':
    collector = CollectHandler()
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')
