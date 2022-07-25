#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/21 13:58
# 广发证券

import concurrent.futures
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.proxy_utils import judge_proxy_is_fail
from bs4 import BeautifulSoup
from utils import remove_file
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder, date_to_stamp
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

data_source = '广发证券'


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
                    if business_type == 4:
                        # 广发证券融资标的证券采集
                        cls.rz_target_collect(search_date)
                    elif business_type == 5:
                        # 广发证券融券标的证券采集
                        cls.rq_target_collect(search_date)
                    elif business_type == 2:
                        # 广发证券可充抵保证金采集
                        cls.guaranty_collect(search_date)
                    else:
                        logger.error(f'business_type{business_type}输入有误，请检查！')

                break
            except Exception as e:
                time.sleep(3)
                logger.error(e)

            max_retry += 1

    @classmethod
    def rz_target_collect(cls, search_date):
        logger.info(f'开始采集广发证券融资标的证券数据{search_date}')
        url = 'http://www.gf.com.cn/business/finance/targetlist'
        page = 1
        page_size = 20
        is_continue = True
        total = None
        retry_count = 30
        data_list = []
        data_title = ['stock_name', 'stock_code', 'rate', 'date']
        start_dt = datetime.datetime.now()
        proxies = super().get_proxies()
        while is_continue:
            params = {"pageSize": page_size, "pageNum": page, "type": 'fin', 'dir': 'asc', 'init_date': search_date,
                      'sort': 'init_date', 'key': None}
            try:
                response = super().get_response(url, proxies, 0, get_headers(), params)
                if response.status_code == 200:
                    text = json.loads(response.text)
                    total = text['count']
                    result = text['result']
                    soup = BeautifulSoup(result, 'html.parser')
                    dom_td_list = soup.select('td')
                else:
                    logger.error(f'请求失败，respones.status={response.status_code}')
                    raise Exception(f'请求失败，respones.status={response.status_code}')

            except Exception as e:
                logger.error(e)
                result = []
                dom_td_list = []
                if retry_count > 0 and judge_proxy_is_fail(e, proxies['http']):
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if total is not None and type(total) is not str and total > page * page_size:
                is_continue = True
                page = page + 1
            else:
                if (len(result) == 0 or total == 0) and retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(3)
                    continue
                is_continue = False

            for i in range(0, len(dom_td_list) - 1, 4):
                dom_span_list = dom_td_list[i].find_all('span')
                stock_name = dom_span_list[0].get_text()
                stock_code = dom_span_list[1].get_text()
                rate = dom_td_list[i + 1].get_text()
                date = dom_td_list[i + 2].get_text()
                data_list.append((stock_name, stock_code, rate, date))
                logger.info(f'已采集数据条数为：{int(len(data_list))}')

        logger.info(f'采集广发证券融资标的证券数据共{int(len(data_list))}条')
        df_result = super().data_deal(data_list, data_title)
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        if int(len(data_list)) == int(total):
            super().data_insert(int(len(data_list)), df_result, search_date,
                                exchange_mt_financing_underlying_security,
                                data_source, start_dt, end_dt, used_time, url)
            logger.info(f'入库信息,共{int(len(data_list))}条')
        else:
            raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{int(total)}不一致，入库失败')

        message = "广发证券融资标的证券数据采集完成"
        super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                  exchange_mt_financing_underlying_security, data_source, message)

        logger.info("广发证券融资标的证券数据采集完成")

    @classmethod
    def rq_target_collect(cls, search_date):
        logger.info(f'开始采集广发证券融券标的证券数据{search_date}')
        url = 'http://www.gf.com.cn/business/finance/targetlist'
        page = 1
        page_size = 20
        is_continue = True
        total = None
        retry_count = 30
        data_list = []
        data_title = ['stock_name', 'stock_code', 'rate', 'date']
        start_dt = datetime.datetime.now()
        proxies = super().get_proxies()
        while is_continue:
            params = {"pageSize": page_size, "pageNum": page, "type": 'slo', 'dir': 'asc', 'init_date': search_date,
                      'sort': 'init_date', 'key': None}
            try:
                response = super().get_response(url, proxies, 0, get_headers(), params)
                if response.status_code == 200:
                    text = json.loads(response.text)
                    total = text['count']
                    result = text['result']
                    soup = BeautifulSoup(result, 'html.parser')
                    dom_td_list = soup.select('td')
                else:
                    logger.error(f'请求失败，respones.status={response.status_code}')
                    raise Exception(f'请求失败，respones.status={response.status_code}')

            except Exception as e:
                logger.error(e)
                result = []
                dom_td_list = []
                if retry_count > 0 and judge_proxy_is_fail(e, proxies['http']):
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if total is not None and type(total) is not str and total > page * page_size:
                is_continue = True
                page = page + 1
            else:
                if (len(result) == 0 or total == 0) and retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(3)
                    continue
                is_continue = False

            for i in range(0, len(dom_td_list) - 1, 4):
                dom_span_list = dom_td_list[i].find_all('span')
                stock_name = dom_span_list[0].get_text()
                stock_code = dom_span_list[1].get_text()
                rate = dom_td_list[i + 1].get_text()
                date = dom_td_list[i + 2].get_text()
                data_list.append((stock_name, stock_code, rate, date))
                logger.info(f'已采集数据条数为：{int(len(data_list))}')

        logger.info(f'采集广发证券融券标的证券数据共{int(len(data_list))}条')
        df_result = super().data_deal(data_list, data_title)
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        if int(len(data_list)) == int(total):
            super().data_insert(int(len(data_list)), df_result, search_date,
                                exchange_mt_lending_underlying_security,
                                data_source, start_dt, end_dt, used_time, url)
            logger.info(f'入库信息,共{int(len(data_list))}条')
        else:
            raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{int(total)}不一致，入库失败')

        message = "广发证券融券标的证券数据采集完成"
        super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                  exchange_mt_lending_underlying_security, data_source, message)

        logger.info("广发证券融券标的证券数据采集完成")

    @classmethod
    def guaranty_collect(cls, search_date):
        logger.info(f'开始采集广发证券可充抵保证金证券数据{search_date}')
        url = 'http://www.gf.com.cn/business/finance/ratiolist'
        page = 1
        page_size = 20
        is_continue = True
        total = None
        retry_count = 30
        data_list = []
        data_title = ['stock_name', 'stock_code', 'rate', 'date']
        start_dt = datetime.datetime.now()
        proxies = super().get_proxies()
        while is_continue:
            params = {"pageSize": page_size, "pageNum": page, 'dir': 'asc', 'init_date': search_date,
                      'sort': 'init_date', 'key': None}
            try:
                response = super().get_response(url, proxies, 0, get_headers(), params)
                if response.status_code == 200:
                    text = json.loads(response.text)
                    total = text['count']
                    result = text['result']
                    soup = BeautifulSoup(result, 'html.parser')
                    dom_td_list = soup.select('td')
                else:
                    logger.error(f'请求失败，respones.status={response.status_code}')
                    raise Exception(f'请求失败，respones.status={response.status_code}')

            except Exception as e:
                logger.error(e)
                if not_ignore_error(e):
                    logger.error(f'解析html异常,text={response.text}')
                result = []
                dom_td_list = []
                if retry_count > 0 and judge_proxy_is_fail(e, proxies['http']):
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if total is not None and type(total) is not str and total > page * page_size:
                is_continue = True
                page = page + 1
            else:
                if (len(result) == 0 or total == 0) and retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(3)
                    continue
                is_continue = False

            for i in range(0, len(dom_td_list) - 1, 4):
                dom_span_list = dom_td_list[i].find_all('span')
                stock_name = dom_span_list[0].get_text()
                stock_code = dom_span_list[1].get_text()
                rate = dom_td_list[i + 1].get_text()
                date = dom_td_list[i + 2].get_text()
                data_list.append((stock_name, stock_code, rate, date))
                logger.info(f'已采集数据条数为：{int(len(data_list))}')

        logger.info(f'采集广发证券可充抵保证金证券数据共{int(len(data_list))}条')
        df_result = super().data_deal(data_list, data_title)
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        if int(len(data_list)) == int(total):
            super().data_insert(int(len(data_list)), df_result, search_date,
                                exchange_mt_guaranty_security,
                                data_source, start_dt, end_dt, used_time, url)
            logger.info(f'入库信息,共{int(len(data_list))}条')
        else:
            raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{int(total)}不一致，入库失败')

        message = "广发证券可充抵保证金证券数据采集完成"
        super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                  exchange_mt_guaranty_security, data_source, message)

        logger.info("广发证券可充抵保证金证券数据采集完成")


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(2, '2022-07-12')
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')
