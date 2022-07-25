#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/18 10:09
# 中信证券
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

data_source = '中信证券'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type, search_date=None):
        search_date = search_date if search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            try:
                if business_type:
                    if business_type == 3:
                        # 中信证券标的证券采集
                        cls.target_collect(search_date)
                    elif business_type == 2:
                        # 中信证券可充抵保证金采集
                        cls.guaranty_collect(search_date)
                    else:
                        logger.error(f'business_type{business_type}输入有误，请检查！')

                break
            except Exception as e:
                time.sleep(3)
                logger.error(e)

            max_retry += 1

    @classmethod
    def target_collect(cls, search_date):
        logger.info(f'开始采集中信证券标的证券数据{search_date}')
        url = 'https://kong.citics.com/pub/api/v1/website/rzrq/rzrqObjects'
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '42',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'kong.citics.com',
            'Origin': 'https://pb.citics.com',
            'Referer': 'https://pb.citics.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': random.choice(USER_AGENTS),
        }
        curr_page = 1
        data = {
            'pageSize': 20,
            'currPage': curr_page,
            'searchDate': search_date
        }
        try:
            start_dt = datetime.datetime.now()
            proxies = super().get_proxies()
            response = super().get_response(url, proxies, 1, headers, None, data)
            data_list = []
            data_title = ['stock_code', 'stock_name', 'rz_rate', 'rq_rate', 'date']

            if response.status_code == 200:
                text = json.loads(response.text)
                if text['errorCode'] == '100008':
                    logger.error(f'该日{search_date}为非交易日,无相应数据')
                    raise Exception(f'该日{search_date}为非交易日,无相应数据')

                total = text['data']['totalRecord']
                total_page = int(total / 20) + 1

                for curr_page in range(1, total_page + 1):
                    logger.info(f'当前为第{curr_page}页')
                    data = {
                        'pageSize': 20,
                        'currPage': curr_page,
                        'searchDate': search_date
                    }
                    response = super().get_response(url, proxies, 1, headers, None, data)
                    if response.status_code == 200:
                        text = json.loads(response.text)
                        data = text['data']['data']

                    if data:
                        for i in data:
                            stock_code = i['stockCode']
                            stock_name = i['stockName']
                            rz_rate = i['rzPercent']
                            rq_rate = i['rqPercent']
                            date = i['dataDate']
                            data_list.append((stock_code, stock_name, rz_rate, rq_rate, date))
                            logger.info(f'已采集数据条数为：{int(len(data_list))}')

                logger.info(f'采集中信证券融资融券标的证券数据共{int(len(data_list))}条')
                df_result = super().data_deal(data_list, data_title)
                end_dt = datetime.datetime.now()
                used_time = (end_dt - start_dt).seconds
                if int(len(data_list)) == total:
                    super().data_insert(int(len(data_list)), df_result, search_date,
                                        exchange_mt_underlying_security,
                                        data_source, start_dt, end_dt, used_time, url)
                    logger.info(f'入库信息,共{int(len(data_list))}条')
                else:
                    raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{total}不一致，入库失败')

                message = "中信证券融资融券标的证券数据采集完成"
                super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                          exchange_mt_underlying_security, data_source, message)

                logger.info("中信证券融资融券标的证券数据采集完成")
            else:
                logger.error(f'请求失败，respones.status={response.status_code}')
                raise Exception(f'请求失败，respones.status={response.status_code}')

        except Exception as e:
            logger.error(e)

    @classmethod
    def guaranty_collect(cls, search_date):
        logger.info(f'开始采集中信证券可充抵保证金比例数据{search_date}')
        url = 'https://kong.citics.com/pub/api/v1/website/rzrq/punching'
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '42',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'kong.citics.com',
            'Origin': 'https://pb.citics.com',
            'Referer': 'https://pb.citics.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': random.choice(USER_AGENTS),
        }
        curr_page = 1
        data = {
            'pageSize': 20,
            'currPage': curr_page,
            'searchDate': search_date
        }
        try:
            start_dt = datetime.datetime.now()
            proxies = super().get_proxies()
            response = super().get_response(url, proxies, 1, headers, None, data)
            data_list = []
            data_title = ['market', 'stock_code', 'stock_name', 'rate', 'date', 'status', 'stockgroup_name']

            if response.status_code == 200:
                text = json.loads(response.text)
                if text['errorCode'] == '100008':
                    logger.error(f'该日{search_date}为非交易日,无相应数据')
                    raise Exception(f'该日{search_date}为非交易日,无相应数据')

                total = text['data']['totalRecord']
                total_page = int(total / 20) + 1

                for curr_page in range(1, total_page + 1):
                    logger.info(f'当前为第{curr_page}页')
                    data = {
                        'pageSize': 20,
                        'currPage': curr_page,
                        'searchDate': search_date
                    }
                    response = super().get_response(url, proxies, 1, headers, None, data)
                    if response.status_code == 200:
                        text = json.loads(response.text)
                        data = text['data']['data']

                    if data:
                        for i in data:
                            market = i['exchangeCode']
                            stock_code = i['stockCode']
                            stock_name = i['stockName']
                            rate = i['percent']
                            date = i['dataDate']
                            status = i['status']
                            stockgroup_name = i['stockgroup_name']
                            data_list.append((market, stock_code, stock_name, rate, date, status, stockgroup_name))
                            logger.info(f'已采集数据条数为：{int(len(data_list))}')

                logger.info(f'采集中信证券可充抵保证金证券数据共{int(len(data_list))}条')
                df_result = super().data_deal(data_list, data_title)
                end_dt = datetime.datetime.now()
                used_time = (end_dt - start_dt).seconds
                if int(len(data_list)) == total:
                    super().data_insert(int(len(data_list)), df_result, search_date,
                                        exchange_mt_guaranty_security,
                                        data_source, start_dt, end_dt, used_time, url)
                    logger.info(f'入库信息,共{int(len(data_list))}条')
                else:
                    raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{total}不一致，入库失败')

                message = "中信证券可充抵保证金证券数据采集完成"
                super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                          exchange_mt_guaranty_security, data_source, message)

                logger.info("中信证券可充抵保证金证券数据采集完成")
            else:
                logger.error(f'请求失败，respones.status={response.status_code}')
                raise Exception(f'请求失败，respones.status={response.status_code}')

        except Exception as e:
            logger.error(e)


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(search_date='2022-07-18')
    # collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')