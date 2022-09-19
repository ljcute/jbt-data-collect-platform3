#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 10:19
# 招商证券 --interface
import os
import sys
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder
import json
import time
from constants import *
from utils.logs_utils import logger
import datetime


exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '招商证券'
url_ = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstock'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type):
        max_retry = 0
        while max_retry < 5:
            logger.info(f'重试第{max_retry}次')
            if business_type:
                if business_type == 4:
                    try:
                        # 招商证券融资标的证券采集
                        cls.rz_target_collect(max_retry)
                        break
                    except ProxyTimeOutEx as es:
                        pass
                    except Exception as e:
                        logger.error(f'{data_source}融资标的证券采集任务异常，请求url为：{url_}，具体异常信息为：{traceback.format_exc()}')
                elif business_type == 5:
                    try:
                        # 招商证券融券标的证券采集
                        cls.rq_target_collect(max_retry)
                        break
                    except ProxyTimeOutEx as es:
                        pass
                    except Exception as e:
                        logger.error(f'{data_source}融券标的证券采集任务异常，请求url为：{url_}，具体异常信息为：{traceback.format_exc()}')
                elif business_type == 2:
                    try:
                        # 招商证券保证金证券
                        cls.guaranty_collect(max_retry)
                        break
                    except ProxyTimeOutEx as es:
                        pass
                    except Exception as e:
                        logger.error(f'{data_source}保证金证券采集任务异常，请求url为：{url_}，具体异常信息为：{traceback.format_exc()}')

            max_retry += 1

    # 标的证券及保证金比例采集
    @classmethod
    def rz_target_collect(cls, max_retry):
        actual_date = datetime.date.today()
        logger.info(f'开始采集招商证券标的证券及保证金比例数据{actual_date}')
        url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstock'
        page_size = random_page_size()
        params = {"pageSize": page_size, "pageNum": 1, "rqbdflag": 1}  # rqbdflag = 1融资
        start_dt = datetime.datetime.now()
        try:
            proxies = super().get_proxies()
            response = super().get_response(data_source, url, proxies, 0, get_headers(), params)
            if response is None or response.status_code != 200:
                raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
            text = json.loads(response.text)
            total = text['body']['totalNum']
            data_list = text['body']['stocks']
            target_title = ['market', 'stock_code', 'stock_name', 'margin_rate']
            target_list = []
            for i in data_list:
                stock_code = i['stkcode']
                stock_name = i['stkname']
                margin_rate = i['marginratefund']
                market = '沪市' if i['market'] == '1' else '深市'
                target_list.append((market, stock_code, stock_name, margin_rate))

            logger.info(f'采集招商证券标的证券及保证金比例数据结束共{int(len(target_list))}条')
            df_result = super().data_deal(target_list, target_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if df_result is not None and int(total) == int(len(target_list)):
                data_status = 1
                super().data_insert(int(len(target_list)), df_result, actual_date,
                                    exchange_mt_financing_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(target_list))}条')
            elif int(total) != int(len(target_list)):
                data_status = 2
                super().data_insert(int(len(target_list)), df_result, actual_date,
                                    exchange_mt_financing_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(target_list))}条')

            message = "zs_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_financing_underlying_security, data_source, message)

            logger.info("招商证券标的证券数据采集完成")
        except Exception as e:
            if max_retry == 4:
                data_status = 2
                super().data_insert(0, str(e), actual_date, exchange_mt_financing_underlying_security,
                                    data_source, start_dt, None, None, url, data_status)

            raise Exception(e)

    @classmethod
    def rq_target_collect(cls, max_retry):
        actual_date = datetime.date.today()
        logger.info(f'开始采集招商证券标的证券及保证金比例数据{actual_date}')
        url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstock'
        page_size = random_page_size()
        params = {"pageSize": page_size, "pageNum": 1, "rqbdflag": 2}  # rqbdflag = 1融资,2融券
        start_dt = datetime.datetime.now()
        try:
            proxies = super().get_proxies()
            response = super().get_response(data_source, url, proxies, 0, get_headers(), params)
            if response is None or response.status_code != 200:
                raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
            text = json.loads(response.text)
            total = text['body']['totalNum']
            data_list = text['body']['stocks']
            target_title = ['market', 'stock_code', 'stock_name', 'margin_rate']
            target_list = []
            for i in data_list:
                stock_code = i['stkcode']
                stock_name = i['stkname']
                margin_rate = i['marginratefund']
                market = '沪市' if i['market'] == '1' else '深市'

                target_list.append((market, stock_code, stock_name, margin_rate))

            logger.info(f'采集招商证券标的证券及保证金比例数据结束共{int(len(target_list))}条')
            df_result = super().data_deal(target_list, target_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if df_result is not None and int(total) == int(len(target_list)):
                data_status = 1
                super().data_insert(int(len(target_list)), df_result, actual_date,
                                    exchange_mt_lending_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(target_list))}条')
            elif int(total) != int(len(target_list)):
                data_status = 2
                super().data_insert(int(len(target_list)), df_result, actual_date,
                                    exchange_mt_lending_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(target_list))}条')

            message = "zs_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_lending_underlying_security, data_source, message)

            logger.info("招商证券标的证券数据采集完成")
        except Exception as e:
            if max_retry == 4:
                data_status = 2
                super().data_insert(0, str(e), actual_date, exchange_mt_lending_underlying_security,
                                    data_source, start_dt, None, None, url, data_status)

            raise Exception(e)


    # 可充抵保证金证券及折算率采集
    @classmethod
    def guaranty_collect(cls, max_retry):
        actual_date = datetime.date.today()
        logger.info(f'开始采集招商证券可充抵保证金证券例数据{actual_date}')
        url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstockdiscount'
        page_size = random_page_size()
        params = {"pageSize": page_size, "pageNum": 1}
        start_dt = datetime.datetime.now()
        try:
            proxies = super().get_proxies()
            response = super().get_response(data_source, url, proxies, 0, get_headers(), params)
            if response is None or response.status_code != 200:
                raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
            text = json.loads(response.text)
            total = text['body']['totalNum']
            data_list = text['body']['stocks']
            target_title = ['market', 'stock_code', 'stock_name', 'discount_rate']
            target_list = []
            for i in data_list:
                stock_code = i['stkcode']
                stock_name = i['stkname']
                margin_rate = i['pledgerate']
                market = '沪市' if i['market'] == '1' else '深市'
                target_list.append((market, stock_code, stock_name, margin_rate))

            logger.info(f'采集招商证券可充抵保证金证券数据结束共{int(len(target_list))}条')
            df_result = super().data_deal(target_list, target_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if df_result is not None and int(total) == int(len(target_list)):
                data_status = 1
                super().data_insert(int(len(target_list)), df_result, actual_date,
                                    exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(target_list))}条')
            elif int(total) != int(len(target_list)):
                data_status = 2
                super().data_insert(int(len(target_list)), df_result, actual_date,
                                    exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(target_list))}条')

            message = "zs_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_guaranty_security, data_source, message)

            logger.info("招商证券保证金证券数据采集完成")
        except Exception as e:
            if max_retry == 4:
                data_status = 2
                super().data_insert(0, str(e), actual_date, exchange_mt_guaranty_security,
                                    data_source, start_dt, None, None, url, data_status)

            raise Exception(e)


def random_page_size(mu=28888, sigma=78888):
    """
    获取随机分页数
    :param mu:
    :param sigma:
    :return:
    """
    random_value = random.randint(mu, sigma)  # Return random integer in range [a, b], including both end points.
    return random_value


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(3)
    collector.collect_data(eval(sys.argv[1]))
