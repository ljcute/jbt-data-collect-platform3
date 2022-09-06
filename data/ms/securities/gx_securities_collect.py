#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/20 10:38
# 国信证券

import os
import sys

from utils.exceptions_utils import ProxyTimeOutEx

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils import remove_file
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

data_source = '国信证券'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type, search_date=None):
        search_date = search_date if search_date is not None else datetime.date.today()
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            try:
                if business_type:
                    if business_type == 4:
                        # 国信证券融资标的证券采集
                        cls.rz_target_collect(search_date)
                    elif business_type == 5:
                        # 国信证券融券标的证券采集
                        cls.rq_target_collect(search_date)
                    elif business_type == 2:
                        # 国信证券可充抵保证金采集
                        cls.guaranty_collect(search_date)
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
    def rz_target_collect(cls, search_date):
        logger.info(f'开始采集国信证券融资标的证券数据{search_date}')
        url = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqbdzq/1.0?type=0'
        page = 1
        page_size = remove_file.random_page_size()
        params = {"pageSize": page_size, "pageNo": page, "rq": search_date}  # type=0表示融资

        try:
            start_dt = datetime.datetime.now()
            data_list = []
            data_title = ['market', 'stock_code', 'stock_name', 'rz_rate']
            proxies = super().get_proxies()
            response = requests.post(url, params, proxies)
            if response.status_code == 200:
                text = json.loads(response.text)
                all_data_list = text['data']
                total_count = text['data'][0]['count']
                if all_data_list:
                    for i in all_data_list:
                        market = '深圳' if str(i['sc']) == '1' else '上海'
                        stock_code = i['zqdm']
                        stock_name = i['zqmc']
                        rate = i['rzbzjbl']
                        data_list.append((market, stock_code, stock_name, rate))

                    logger.info(f'采集国信证券融资标的证券数据共total_data_list:{len(data_list)}条')
                    df_result = super().data_deal(data_list, data_title)
                    end_dt = datetime.datetime.now()
                    used_time = (end_dt - start_dt).seconds
                    if int(len(data_list)) == int(total_count) and int(len(data_list)) > 0 and int(total_count)>0:
                        super().data_insert(int(len(data_list)), df_result, search_date,
                                            exchange_mt_financing_underlying_security,
                                            data_source, start_dt, end_dt, used_time, url)
                        logger.info(f'入库信息,共{int(len(data_list))}条')
                    else:
                        raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{int(total_count)}不一致，采集程序存在抖动，需要重新采集')

                    message = "gx_securities_collect"
                    super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                              exchange_mt_financing_underlying_security, data_source, message)

                    logger.info("国信证券融资标的证券数据采集完成")
            else:
                logger.error(f'请求失败，respones.status={response.status_code}')
                raise Exception(f'请求失败，respones.status={response.status_code}')
        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            logger.error(e)

    @classmethod
    def rq_target_collect(cls, search_date):
        logger.info(f'开始采集国信证券融券标的证券数据{search_date}')
        url = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqbdzq/1.0?type=1'
        page = 1
        page_size = remove_file.random_page_size()
        params = {"pageSize": page_size, "pageNo": page, "rq": search_date}  # type=0表示融资

        try:
            start_dt = datetime.datetime.now()
            data_list = []
            data_title = ['market', 'stock_code', 'stock_name', 'rq_rate']
            proxies = super().get_proxies()
            response = requests.post(url, params, proxies)
            if response.status_code == 200:
                text = json.loads(response.text)
                all_data_list = text['data']
                total_count = text['data'][0]['count']
                if all_data_list:
                    for i in all_data_list:
                        market = '深圳' if str(i['sc']) == '1' else '上海'
                        stock_code = i['zqdm']
                        stock_name = i['zqmc']
                        rate = i['rzbzjbl']
                        data_list.append((market, stock_code, stock_name, rate))

                    logger.info(f'采集国信证券融券标的证券数据共total_data_list:{len(data_list)}条')
                    df_result = super().data_deal(data_list, data_title)
                    end_dt = datetime.datetime.now()
                    used_time = (end_dt - start_dt).seconds
                    if int(len(data_list)) == int(total_count) and int(len(data_list)) > 0 and int(total_count) > 0 :
                        super().data_insert(int(len(data_list)), df_result, search_date,
                                            exchange_mt_lending_underlying_security,
                                            data_source, start_dt, end_dt, used_time, url)
                        logger.info(f'入库信息,共{int(len(data_list))}条')
                    else:
                        raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{int(total_count)}不一致，采集程序存在抖动，需要重新采集')

                    message = "gx_securities_collect"
                    super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                              exchange_mt_lending_underlying_security, data_source, message)

                    logger.info("国信证券融券标的证券数据采集完成")
                else:
                    logger.error(f'请求失败，respones.status={response.status_code}')
                    raise Exception(f'请求失败，respones.status={response.status_code}')
        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            logger.error(e)

    @classmethod
    def guaranty_collect(cls, search_date):
        logger.info(f'开始采集国信证券可充抵保证金证券数据{search_date}')
        url = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqkcdbzjzq/1.0'
        page = 1
        page_size = remove_file.random_page_size()
        params = {"pageSize": page_size, "pageNo": page, "rq": search_date}  # type=0表示融资

        try:
            start_dt = datetime.datetime.now()
            data_list = []
            data_title = ['stock_code', 'stock_name', 'zsl']
            proxies = super().get_proxies()
            response = requests.post(url, params, proxies)
            if response.status_code == 200:
                text = json.loads(response.text)
                all_data_list = text['data']
                total_count = text['data'][0]['count']
                if all_data_list:
                    for i in all_data_list:
                        stock_code = i['zqdm']
                        stock_name = i['zqmc']
                        zsl = i['zsl']
                        data_list.append((stock_code, stock_name, zsl))

                    logger.info(f'采集国信证券可充抵保证金证券数据共total_data_list:{len(data_list)}条')
                    df_result = super().data_deal(data_list, data_title)
                    end_dt = datetime.datetime.now()
                    used_time = (end_dt - start_dt).seconds
                    if int(len(data_list)) == int(total_count) and int(len(data_list)) > 0 and int(total_count) > 0:
                        super().data_insert(int(len(data_list)), df_result, search_date,
                                            exchange_mt_guaranty_security,
                                            data_source, start_dt, end_dt, used_time, url)
                        logger.info(f'入库信息,共{int(len(data_list))}条')
                    else:
                        raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{int(total_count)}不一致，采集程序存在抖动，需要重新采集')

                    message = "gx_securities_collect"
                    super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                              exchange_mt_guaranty_security, data_source, message)

                    logger.info("国信证券可充抵保证金证券数据采集完成")
            else:
                logger.error(f'请求失败，respones.status={response.status_code}')
                raise Exception(f'请求失败，respones.status={response.status_code}')
        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            logger.error(e)


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(5, '2022-07-01')
    # collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')
