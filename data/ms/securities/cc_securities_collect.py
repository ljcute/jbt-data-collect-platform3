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
        try:
            proxies = super().get_proxies()
            while is_continue:
                params = {"page": page, "channelid": 257420, "searchword": 'KGNyZWRpdGZ1bmRjdHJsPTAp',
                          "_": get_timestamp()}
                response = super().get_response(data_source, url, proxies, 0, cc_headers, params)
                if response is None or response.status_code != 200:
                    raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
                text = json.loads(response.text)
                logger.info("开始处理融资标的券数据")
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
            df_result = super().data_deal(data_list, title_list)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            self.record_num = int(len(data_list))
            if int(len(data_list)) == total and int(len(data_list)) > 0 and total > 0:
                data_status = 1
                super().data_insert(int(len(data_list)), df_result, actual_date,
                                    exchange_mt_financing_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(data_list))}条')
            elif int(len(data_list)) != total:
                data_status = 2
                super().data_insert(int(len(data_list)), df_result, actual_date,
                                    exchange_mt_financing_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(data_list))}条')

            message = "cc_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_financing_underlying_security, data_source, message)

            logger.info("长城证券融资标的证券数据采集完成")
        except Exception as e:
            # if max_retry == 4:
            #     data_status = 2
            #     super().data_insert(0, str(e), actual_date, exchange_mt_financing_underlying_security,
            #                         data_source, start_dt, None, None, url, data_status)
            raise Exception(e)

    def rq_underlying_securities_collect(self):
        actual_date = datetime.date.today()
        logger.info(f'开始采集长城证券融券标的券数据{actual_date}')
        url = 'http://www.cgws.com/was5/web/de.jsp'
        page = 1
        page_size = 5
        is_continue = True
        data_list = []
        title_list = ['sec_code', 'sec_name', 'round_rate', 'date', 'market']
        start_dt = datetime.datetime.now()
        try:
            proxies = get_proxies()
            while is_continue:
                params = {"page": page, "channelid": 257420, "searchword": 'KGNyZWRpdHN0a2N0cmw9MCk=',
                          "_": get_timestamp()}
                response = super().get_response(data_source, url, proxies, 0, cc_headers, params)
                if response is None or response.status_code != 200:
                    raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
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
            df_result = super().data_deal(data_list, title_list)
            end_dt = datetime.datetime.now()
            # 计算采集数据所需时间used_time
            used_time = (end_dt - start_dt).seconds
            if int(len(data_list)) == total and int(len(data_list)) > 0 and total > 0:
                data_status = 1
                super().data_insert(int(len(data_list)), df_result, actual_date,
                                    exchange_mt_lending_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(data_list))}条')
            elif int(len(data_list)) != total:
                data_status = 2
                super().data_insert(int(len(data_list)), df_result, actual_date,
                                    exchange_mt_financing_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(data_list))}条')
            message = "cc_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_lending_underlying_security, data_source, message)

            logger.info("长城证券融券标的证券数据采集完成")
        except Exception as e:
            # if max_retry == 4:
            #     data_status = 2
            #     super().data_insert(0, str(e), actual_date, exchange_mt_financing_underlying_security,
            #                         data_source, start_dt, None, None, url, data_status)

            raise Exception(e)

    def guaranty_securities_collect(self):
        actual_date = datetime.date.today()
        logger.info(f'开始采集长城证券担保券数据{actual_date}')
        url = 'http://www.cgws.com/was5/web/de.jsp'
        page = 1
        page_size = 5
        is_continue = True
        data_list = []
        title_list = ['sec_code', 'sec_name', 'round_rate', 'date', 'market']
        start_dt = datetime.datetime.now()
        try:
            proxies = get_proxies()
            while is_continue:
                params = {"page": page, "channelid": 229873, "searchword": None,
                          "_": get_timestamp()}
                response = super().get_response(data_source, url, proxies, 0, cc_headers, params)
                if response is None or response.status_code != 200:
                    raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
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
            df_result = super().data_deal(data_list, title_list)
            end_dt = datetime.datetime.now()
            # 计算采集数据所需时间used_time
            used_time = (end_dt - start_dt).seconds
            if int(len(data_list)) == total and int(len(data_list)) > 0 and total > 0:
                data_status = 1
                super().data_insert(int(len(data_list)), df_result, actual_date, exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(data_list))}条')
            elif int(len(data_list)) != total:
                data_status = 2
                super().data_insert(int(len(data_list)), df_result, actual_date, exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(data_list))}条')

            message = "cc_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_guaranty_security, data_source, message)

            logger.info("长城证券担保券数据采集完成")
        except Exception as e:
            # if max_retry == 4:
            #     data_status = 2
            #     super().data_insert(0, str(e), actual_date, exchange_mt_guaranty_security,
            #                         data_source, start_dt, None, None, url, data_status)

            raise Exception(e)


def get_timestamp():
    return int(time.time() * 1000)


if __name__ == '__main__':
    collector = CollectHandler()
    collector.collect_data(2)
    # collector.collect_data(eval(sys.argv[1]))
