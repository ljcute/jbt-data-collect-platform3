#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/07/01 13:19
# 长城证券
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler, get_proxies
from utils.deal_date import ComplexEncoder
import json
import time

from utils.logs_utils import logger
import datetime

cc_headers = {
    'Connection': 'close',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
}

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '长城证券'
url = 'http://www.cgws.com/was5/web/de.jsp'


class CollectHandler(BaseHandler):

    def __init__(self):
        BaseHandler.__init__(self, data_source, url)
        self.data_source = data_source
        self.url = url

    def rz_underlying_securities_collect(self):
        actual_date = datetime.date.today()
        url = 'http://www.cgws.com/was5/web/de.jsp'
        page = 1
        page_size = 5
        is_continue = True
        data_list = []
        title_list = ['sec_code', 'sec_name', 'round_rate', 'date', 'market']
        start_dt = datetime.datetime.now()
        proxies = BaseHandler.get_proxies(self)
        while is_continue:
            params = {"page": page, "channelid": 257420, "searchword": 'KGNyZWRpdGZ1bmRjdHJsPTAp',
                      "_": get_timestamp()}
            response = BaseHandler.get_response(self, url, proxies, 0, cc_headers, params)
            # retry_count = 3
            # if response is None or response.status_code != 200:
            #     while retry_count > 0:
            #         response = BaseHandler.get_response(self, url, proxies, 0, cc_headers, params)
            #         if response is not None:
            #             break
            #         else:
            #             retry_count = retry_count - 1
            #             continue
            text = json.loads(response.text)
            row_list = text['rows']
            total = 0
            if row_list:
                total = int(text['total'])
            if total is not None and type(total) is not str and total > page * page_size:
                is_continue = True
                page = page + 1
            else:
                is_continue = False
            for i in row_list:
                sec_code = i['code']
                if sec_code == "":  # 一页数据,遇到{"code":"","name":"","rate":"","pub_date":"","market":""}表示完结
                    break
                u_name = i['name'].replace('u', '\\u')  # 把u4fe1u7acbu6cf0转成\\u4fe1\\u7acb\\u6cf0
                sec_name = u_name.encode().decode('unicode_escape')  # unicode转汉字
                market = '深圳' if i['market'] == "0" else '上海'
                round_rate = i['rate']
                date = i['pub_date']
                data_list.append((sec_code, sec_name, round_rate, date, market))
            logger.info(f'已采集数据条数：{int(len(data_list))}')

        logger.info(f'采集融资标的券数据结束,共{int(len(data_list))}条')
        df_result = BaseHandler.rz_underlying_securities_collect(self, data_list, title_list)
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        BaseHandler.verify_data_record_num(self, int(len(data_list)), total, df_result, actual_date,
                                           exchange_mt_financing_underlying_security, data_source
                                           , start_dt, end_dt, used_time, url)

        message = "cc_securities_collect"
        BaseHandler.kafka_mq_producer(self, json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_financing_underlying_security, data_source, message)

    def rq_underlying_securities_collect(self):
        actual_date = datetime.date.today()
        url = 'http://www.cgws.com/was5/web/de.jsp'
        page = 1
        page_size = 5
        is_continue = True
        data_list = []
        title_list = ['sec_code', 'sec_name', 'round_rate', 'date', 'market']
        start_dt = datetime.datetime.now()
        proxies = get_proxies()
        while is_continue:
            params = {"page": page, "channelid": 257420, "searchword": 'KGNyZWRpdHN0a2N0cmw9MCk=',
                      "_": get_timestamp()}
            response = BaseHandler.get_response(self, url, proxies, 0, cc_headers, params)
            text = json.loads(response.text)
            row_list = text['rows']
            total = 0
            if row_list:
                total = int(text['total'])
            if total is not None and type(total) is not str and total > page * page_size:
                is_continue = True
                page = page + 1
            else:
                is_continue = False
            for i in row_list:
                sec_code = i['code']
                if sec_code == "":  # 一页数据,遇到{"code":"","name":"","rate":"","pub_date":"","market":""}表示完结
                    break
                u_name = i['name'].replace('u', '\\u')  # 把u4fe1u7acbu6cf0转成\\u4fe1\\u7acb\\u6cf0
                sec_name = u_name.encode().decode('unicode_escape')  # unicode转汉字
                market = '深圳' if i['market'] == "0" else '上海'
                round_rate = i['rate']
                date = i['pub_date']
                data_list.append((sec_code, sec_name, round_rate, date, market))
            logger.info(f'已采集数据条数：{int(len(data_list))}')

        logger.info(f'采集融券标的券数据结束,共{int(len(data_list))}条')
        df_result = BaseHandler.rq_underlying_securities_collect(self, data_list, title_list)
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        BaseHandler.verify_data_record_num(self, int(len(data_list)), total, df_result, actual_date,
                                           exchange_mt_lending_underlying_security,
                                           data_source, start_dt, end_dt, used_time, url)

        message = "cc_securities_collect"
        BaseHandler.kafka_mq_producer(self, json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_lending_underlying_security, data_source, message)

    def guaranty_securities_collect(self):
        actual_date = datetime.date.today()
        url = 'http://www.cgws.com/was5/web/de.jsp'
        page = 1
        page_size = 5
        is_continue = True
        data_list = []
        title_list = ['sec_code', 'sec_name', 'round_rate', 'date', 'market']
        start_dt = datetime.datetime.now()
        proxies = get_proxies()
        while is_continue:
            params = {"page": page, "channelid": 229873, "searchword": None,
                      "_": get_timestamp()}
            response = BaseHandler.get_response(self, url, proxies, 0, cc_headers, params)
            text = json.loads(response.text)
            row_list = text['rows']
            total = 0
            if row_list:
                total = int(text['total'])
            if total is not None and type(total) is not str and total > page * page_size:
                is_continue = True
                page = page + 1
            else:
                is_continue = False
            for i in row_list:
                sec_code = i['code']
                if sec_code == "":  # 一页数据,遇到{"code":"","name":"","rate":"","pub_date":"","market":""}表示完结
                    break
                u_name = i['name'].replace('u', '\\u')  # 把u4fe1u7acbu6cf0转成\\u4fe1\\u7acb\\u6cf0
                sec_name = u_name.encode().decode('unicode_escape')  # unicode转汉字
                market = '深圳' if i['market'] == "0" else '上海'
                round_rate = i['rate']
                date = i['pub_date']
                data_list.append((sec_code, sec_name, round_rate, date, market))
            logger.info(f'已采集数据条数：{int(len(data_list))}')

        logger.info(f'采集担保券数据结束,共{int(len(data_list))}条')
        df_result = BaseHandler.guaranty_securities_collect(self, data_list, title_list)
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        BaseHandler.verify_data_record_num(self, int(len(data_list)), total, df_result, actual_date,
                                           exchange_mt_guaranty_security,
                                           data_source, start_dt, end_dt, used_time, url)

        message = "cc_securities_collect"
        BaseHandler.kafka_mq_producer(self, json.dumps(actual_date, cls=ComplexEncoder),
                                  exchange_mt_guaranty_security, data_source, message)


def get_timestamp():
    return int(time.time() * 1000)


if __name__ == '__main__':
    collector = CollectHandler()
    collector.collect_data(5)
    # collector.collect_data(eval(sys.argv[1]))
