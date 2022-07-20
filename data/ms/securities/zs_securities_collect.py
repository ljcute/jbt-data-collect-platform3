#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 10:19
# 招商证券 --interface
import os
import sys



BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)


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


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type):
        max_retry = 0
        while max_retry < 3:
            try:
                if business_type:
                    if business_type == 3:
                        # 招商证券标的证券采集
                        cls.rz_target_collect()
                    elif business_type == 2:
                        # 招商证券保证金证券
                        cls.guaranty_collect()
                    else:
                        logger.error(f'business_type{business_type}输入有误，请检查！')

                break
            except Exception as e:
                time.sleep(3)
                logger.error(e)

            max_retry += 1

    # 标的证券及保证金比例采集
    @classmethod
    def rz_target_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集招商证券标的证券及保证金比例数据{actual_date}')
        url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstock'
        page_size = random_page_size()
        params = {"pageSize": page_size, "pageNum": 1, "rqbdflag": 1}  # rqbdflag = 1融资
        try:
            start_dt = datetime.datetime.now()
            proxies = super().get_proxies()
            response = super().get_response(url, proxies, 0, get_headers(), params)
            text = json.loads(response.text)
            total = text['body']['totalNum']
            data_list = text['body']['stocks']
            target_title = ['stock_code', 'stock_name', 'margin_rate']
            target_list = []
            for i in data_list:
                stock_code = i['stkcode']
                stock_name = i['stkname']
                margin_rate = i['marginratefund']
                target_list.append((stock_code, stock_name, margin_rate))

            logger.info(f'采集招商证券标的证券及保证金比例数据结束共{int(len(target_list))}条')
            df_result = super().data_deal(target_list, target_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if df_result is not None:
                super().data_insert(int(len(target_list)), df_result, actual_date,
                                    exchange_mt_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(target_list))}条')
            else:
                raise Exception(f'采集数据条数为0，入库失败')

            message = "招商证券标的证券数据采集完成"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_underlying_security, data_source, message)

            logger.info("招商证券标的证券数据采集完成")

        except Exception as es:
            logger.error(es)

    # 可充抵保证金证券及折算率采集
    @classmethod
    def guaranty_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集招商证券可充抵保证金证券例数据{actual_date}')
        url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstockdiscount'
        page_size = random_page_size()
        params = {"pageSize": page_size, "pageNum": 1}
        try:
            start_dt = datetime.datetime.now()
            proxies = super().get_proxies()
            response = super().get_response(url, proxies, 0, get_headers(), params)
            text = json.loads(response.text)
            total = text['body']['totalNum']
            data_list = text['body']['stocks']
            target_title = ['stock_code', 'stock_name', 'discount_rate']
            target_list = []
            for i in data_list:
                stock_code = i['stkcode']
                stock_name = i['stkname']
                margin_rate = i['pledgerate']
                target_list.append((stock_code, stock_name, margin_rate))

            logger.info(f'采集招商证券可充抵保证金证券数据结束共{int(len(target_list))}条')
            df_result = super().data_deal(target_list, target_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if df_result is not None:
                super().data_insert(int(len(target_list)), df_result, actual_date,
                                    exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(target_list))}条')
            else:
                raise Exception(f'采集数据条数为0，入库失败')

            message = "招商证券保证金证券数据采集完成"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_guaranty_security, data_source, message)

            logger.info("招商证券保证金证券数据采集完成")

        except Exception as es:
            logger.error(es)


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
