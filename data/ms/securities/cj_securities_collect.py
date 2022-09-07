#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 13:19
# 长江证券
import os
import sys


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

# 定义常量
broker_id = 10005
exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '长江证券'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type):
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            try:
                if business_type:
                    if business_type == 3:
                        # 长江证券标的证券采集
                        cls.target_collect()
                    elif business_type == 2:
                        # 长江证券可充抵保证金采集
                        cls.guaranty_collect()
                    else:
                        logger.error(f'business_type{business_type}输入有误，请检查！')

                break
            except ProxyTimeOutEx as es:
                pass
            except Exception as e:
                time.sleep(3)
                logger.error(e)

            max_retry += 1

    @classmethod
    def target_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集长江证券标的证券及保证金比例数据{actual_date}')
        url = 'https://www.95579.com/servlet/json'
        params = {"funcNo": "902122", "i_page": 1, "i_perpage": 10000}  # 默认查询当天
        target_title = ['market', 'stock_code', 'stock_name', 'rz_rate', 'rq_rate']
        try:
            proxies = super().get_proxies()
            response = super().get_response(url, proxies, 0, get_headers(), params)
            if response.status_code == 200:
                start_dt = datetime.datetime.now()
                text = json.loads(response.text)
                data_list = text['results']
                target_list = []
                if len(data_list) > 0:
                    total = int(data_list[0]['total_rows'])
                    for i in data_list:
                        stock_code = i['stock_code']
                        stock_name = i['stock_name']
                        rz_rate = i['fin_ratio']
                        rq_rate = i['bail_ratio']
                        market = '深圳' if i['exchange_type'] == '2' else '上海'
                        rzbd = i['rzbd']
                        rqbd = i['rqbd']
                        target_list.append((market, stock_code, stock_name, rz_rate, rq_rate))
                        logger.info(f'已采集数据条数：{int(len(target_list))}')

                logger.info(f'采集长江证券标的证券数据共{int(len(target_list))}条')
                df_result = super().data_deal(target_list, target_title)
                end_dt = datetime.datetime.now()
                used_time = (end_dt - start_dt).seconds
                if int(len(target_list)) == total and int(len(target_list)) > 0 and total > 0:
                    super().data_insert(int(len(target_list)), df_result, actual_date,
                                        exchange_mt_underlying_security,
                                        data_source, start_dt, end_dt, used_time, url)
                    logger.info(f'入库信息,共{int(len(target_list))}条')
                else:
                    raise Exception(f'采集数据条数{int(len(target_list))}与官网数据条数{total}不一致，采集程序存在抖动，需要重新采集')

                # message = "cj_securities_collect"
                # super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                #                           exchange_mt_underlying_security, data_source, message)

                logger.info("长江证券标的证券数据采集完成")
            else:
                logger.info("无长江证券标的证券及保证金比例数据")
                raise Exception("无长江证券标的证券及保证金比例数据")
        except ProxyTimeOutEx as e:
            pass
        except Exception as es:
            logger.error(es)

    @classmethod
    def guaranty_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集长江证券可充抵保证金证券数据{actual_date}')
        url = 'https://www.95579.com/servlet/json'
        params = {"funcNo": "902124", "i_page": 1, "i_perpage": 10000}  # 默认查询当天
        target_title = ['market', 'stock_code', 'stock_name', 'discount_rate']
        try:
            proxies = super().get_proxies()
            response = super().get_response(url, proxies, 0, get_headers(), params)
            if response.status_code == 200:
                start_dt = datetime.datetime.now()
                text = json.loads(response.text)
                data_list = text['results']
                target_list = []
                if len(data_list) > 0:
                    total = int(data_list[0]['total_rows'])
                    for i in data_list:
                        stock_code = i['stock_code']
                        stock_name = i['stock_name']
                        discount_rate = i['assure_ratio']
                        market = '深圳' if i['exchange_type'] == '2' else '上海'
                        target_list.append((market, stock_code, stock_name, discount_rate))
                        logger.info(f'已采集数据条数：{int(len(target_list))}')

                    logger.info(f'采集长江证券可充抵保证金证券数据,共{int(len(target_list))}条')
                    df_result = super().data_deal(target_list, target_title)
                    end_dt = datetime.datetime.now()
                    used_time = (end_dt - start_dt).seconds
                    if int(len(target_list)) == total and int(len(target_list)) > 0 and total > 0:
                        super().data_insert(int(len(target_list)), df_result, actual_date,
                                            exchange_mt_guaranty_security,
                                            data_source, start_dt, end_dt, used_time, url)
                        logger.info(f'入库信息,共{int(len(target_list))}条')
                    else:
                        raise Exception(f'采集数据条数{int(len(target_list))}与官网数据条数{total}不一致，采集程序存在抖动，需要重新采集')

                    message = "cj_securities_collect"
                    super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                              exchange_mt_guaranty_security, data_source, message)

                    logger.info("长江证券可充抵保证金证券数据采集完成")
                else:
                    logger.info("无长江证券可充抵保证金证券数据")
                    raise Exception("无长江证券可充抵保证金证券数据")
        except ProxyTimeOutEx as e:
            pass
        except Exception as es:
            logger.error(es)


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(2)
    collector.collect_data(eval(sys.argv[1]))
