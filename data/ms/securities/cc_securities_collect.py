#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/07/01 13:19
# 长城证券
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler
import json
import time

cc_headers = {
    'Connection': 'close',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
}


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '长城证券'
        self.url = 'http://www.cgws.com/was5/web/de.jsp'

    def rz_underlying_securities_collect(self):
        page = 1
        page_size = 5
        self.data_list = []
        self.title_list = ['sec_code', 'sec_name', 'round_rate', 'date', 'market']
        while True:
            params = {"page": page, "channelid": 257420, "searchword": 'KGNyZWRpdGZ1bmRjdHJsPTAp',
                      "_": get_timestamp()}
            response = self.get_response(self.url, 0, cc_headers, params)
            text = json.loads(response.text)
            row_list = text['rows']
            if row_list:
                self.total_num = int(text['total'])
            for i in row_list:
                sec_code = i['code']
                if sec_code == "":  # 一页数据,遇到{"code":"","name":"","rate":"","pub_date":"","market":""}表示完结
                    break
                u_name = i['name'].replace('u', '\\u')  # 把u4fe1u7acbu6cf0转成\\u4fe1\\u7acb\\u6cf0
                sec_name = u_name.encode().decode('unicode_escape')  # unicode转汉字
                if i['market'] == "0":
                    market = '深圳'
                elif i['market'] == "1":
                    market = '上海'
                else:
                    market = '北京'
                round_rate = i['rate']
                self.biz_dt = i['pub_date']
                self.data_list.append((sec_code, sec_name, round_rate, self.biz_dt, market))
                self.collect_num = int(len(self.data_list))
            if self.total_num is not None and type(self.total_num) is not str and self.total_num > page * page_size:
                page = page + 1
            else:
                break

    def rq_underlying_securities_collect(self):
        page = 1
        page_size = 5
        self.data_list = []
        self.title_list = ['sec_code', 'sec_name', 'round_rate', 'date', 'market']
        while True:
            params = {"page": page, "channelid": 257420, "searchword": 'KGNyZWRpdHN0a2N0cmw9MCk=',
                      "_": get_timestamp()}
            response = self.get_response(self.url, 0, cc_headers, params)
            text = json.loads(response.text)
            row_list = text['rows']
            if row_list:
                self.total_num = int(text['total'])
            for i in row_list:
                sec_code = i['code']
                if sec_code == "":  # 一页数据,遇到{"code":"","name":"","rate":"","pub_date":"","market":""}表示完结
                    break
                u_name = i['name'].replace('u', '\\u')  # 把u4fe1u7acbu6cf0转成\\u4fe1\\u7acb\\u6cf0
                sec_name = u_name.encode().decode('unicode_escape')  # unicode转汉字
                if i['market'] == "0":
                    market = '深圳'
                elif i['market'] == "1":
                    market = '上海'
                else:
                    market = '北京'
                round_rate = i['rate']
                self.biz_dt = i['pub_date']
                self.data_list.append((sec_code, sec_name, round_rate, self.biz_dt, market))
                self.collect_num = int(len(self.data_list))
            if self.total_num is not None and type(self.total_num) is not str and self.total_num > page * page_size:
                page = page + 1
            else:
                break

    def guaranty_securities_collect(self):
        page = 1
        page_size = 5
        self.data_list = []
        self.title_list = ['sec_code', 'sec_name', 'round_rate', 'date', 'market']
        while True:
            params = {"page": page, "channelid": 229873, "searchword": None,
                      "_": get_timestamp()}
            response = self.get_response(self.url, 0, cc_headers, params)
            text = json.loads(response.text)
            row_list = text['rows']
            if row_list:
                self.total_num = int(text['total'])
            for i in row_list:
                sec_code = i['code']
                if sec_code == "":  # 一页数据,遇到{"code":"","name":"","rate":"","pub_date":"","market":""}表示完结
                    break
                u_name = i['name'].replace('u', '\\u')  # 把u4fe1u7acbu6cf0转成\\u4fe1\\u7acb\\u6cf0
                sec_name = u_name.encode().decode('unicode_escape')  # unicode转汉字
                if i['market'] == "0":
                    market = '深圳'
                elif i['market'] == "1":
                    market = '上海'
                else:
                    market = '北京'
                round_rate = i['rate']
                self.biz_dt = i['pub_date']
                self.data_list.append((sec_code, sec_name, round_rate, self.biz_dt, market))
                self.collect_num = int(len(self.data_list))
            if self.total_num is not None and type(self.total_num) is not str and self.total_num > page * page_size:
                page = page + 1
            else:
                break


def get_timestamp():
    return int(time.time() * 1000)


if __name__ == '__main__':
    # CollectHandler().collect_data(eval(sys.argv[1]))
    pass