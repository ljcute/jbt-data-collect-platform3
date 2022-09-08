#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/18 13:48
# 华泰证券
import os
import sys
import concurrent.futures

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.proxy_utils import get_proxies
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

data_source = '华泰证券'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type, search_date=None):
        search_date = search_date if search_date is not None else datetime.date.today()
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            try:
                if business_type:
                    if business_type == 3:
                        # 华泰证券标的证券采集
                        cls.target_collect(search_date)
                    elif business_type == 2:
                        # 华泰证券可充抵保证金采集
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
    def target_collect(cls, search_date):
        logger.info(f'开始采集华泰证券标的证券数据{search_date}')
        url = 'https://www.htsc.com.cn/browser/rzrqPool/getBdZqc.do'
        data = {'date': search_date, 'hsPage': 1, 'hsPageSize': 8, 'ssPage': 1, 'ssPageSize': 8}
        start_dt = datetime.datetime.now()
        proxies = get_proxies(3, 10)
        response = requests.post(url=url, params=data, proxies=proxies, timeout=6)
        db_total_count = None
        if response.status_code == 200:
            text = json.loads(response.text)
            db_hs_count = text['result']['bdHsCount']
            db_ss_count = text['result']['bdSsCount']
            db_total_count = int(db_hs_count) + int(db_ss_count)
        else:
            logger.error(f'请求失败，respones.status={response.status_code}')
            raise Exception(f'请求失败，respones.status={response.status_code}')

        logger.info(f'total:{db_total_count}')
        data_title = ['market', 'stock_code', 'stock_name', 'rz_rate', 'rq_rate']

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_list = []
            about_total_page = 300
            partition_page = 50
            for i in range(0, about_total_page, partition_page):
                start_page = i + 1
                if i >= about_total_page - partition_page:
                    end_page = None  # 最后一页，设为None
                else:
                    end_page = i + partition_page

                future = executor.submit(cls.collect_by_partition_rzrq, start_page, end_page, search_date)
                future_list.append(future)

            total_data_list = []
            for r in future_list:
                data_list = r.result()
                if data_list:
                    total_data_list.extend(data_list)

            logger.info(f'采集华泰证券标的证券数据共total_data_list:{len(total_data_list)}条')
            df_result = super().data_deal(total_data_list, data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if int(len(total_data_list)) == db_total_count and int(len(total_data_list)) > 0 and db_total_count > 0:
                super().data_insert(int(len(total_data_list)), df_result, search_date,
                                    exchange_mt_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(total_data_list))}条')
            else:
                raise Exception(f'采集数据条数{int(len(total_data_list))}与官网数据条数{db_total_count}不一致，采集程序存在抖动，需要重新采集')

            message = "ht_securities_collect"
            super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                      exchange_mt_underlying_security, data_source, message)

            logger.info("华泰证券标的证券数据采集完成")

    @classmethod
    def collect_by_partition_rzrq(cls, start_page, end_page, search_date):
        url = 'https://www.htsc.com.cn/browser/rzrqPool/getBdZqc.do'
        data_list = []
        hs_page = ss_page = start_page
        page_size = 8
        if end_page is None:
            end_page = 100000

        hs_is_continue = ss_is_continue = True
        retry_count = 5
        while (hs_is_continue or ss_is_continue) and hs_page <= end_page and ss_page <= end_page:
            data = {'date': search_date, 'hsPage': hs_page, 'hsPageSize': page_size, 'ssPage': ss_page,
                    'ssPageSize': page_size}
            logger.info("{}".format(data))
            try:
                proxies = get_proxies(3, 10)
                response = requests.post(url=url, params=data, proxies=proxies, timeout=6)
                text = json.loads(response.text)
                hs_data_list = text['result']['bdHs']
                ss_data_list = text['result']['bdSs']
            except Exception as e:
                logger.error(e)
                hs_is_continue = ss_is_continue = False
                if retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if hs_is_continue and len(hs_data_list) > 0:
                for i in hs_data_list:
                    market = '沪市'
                    stock_code = i['stockCode']
                    stock_name = i['stockName']
                    rz_rate = i['finRatio']
                    rq_rate = i['sloRatio']
                    data_list.append((market, stock_code, stock_name, rz_rate, rq_rate))

            if ss_is_continue and len(ss_data_list) > 0:
                for i in ss_data_list:
                    market = '深市'
                    stock_code = i['stockCode']
                    stock_name = i['stockName']
                    rz_rate = i['finRatio']
                    rq_rate = i['sloRatio']
                    data_list.append((market, stock_code, stock_name, rz_rate, rq_rate))

            if len(hs_data_list) == page_size:
                hs_page = hs_page + 1
            else:
                hs_is_continue = False

            if len(ss_data_list) == page_size:
                ss_page = ss_page + 1
            else:
                ss_is_continue = False

        return data_list

    @classmethod
    def guaranty_collect(cls, search_date):
        logger.info(f'开始采集华泰证券可充抵保证金证券数据{search_date}')
        url = 'https://www.htsc.com.cn/browser/rzrqPool/getDbZqc.do'
        data = {'date': search_date, 'hsPage': 1, 'hsPageSize': 8, 'ssPage': 1,
                'ssPageSize': 8}
        proxies = get_proxies(3, 10)
        response = requests.post(url=url, params=data, proxies=proxies, timeout=6)
        if response.status_code == 200:
            text = json.loads(response.text)
            db_hs_count = text['result']['dbHsCount']
            db_ss_count = text['result']['dbSsCount']
            db_total_count = int(db_hs_count) + int(db_ss_count)
        else:
            logger.error(f'请求失败，respones.status={response.status_code}')
            raise Exception(f'请求失败，respones.status={response.status_code}')
        logger.info(f'total:{db_total_count}')
        data_title = ['market', 'stock_code', 'stock_name', 'rate', 'stock_group_name']
        db_total_count = []
        start_dt = datetime.datetime.now()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_list = []
            about_total_page = 1600
            partition_page = 200
            for i in range(0, about_total_page, partition_page):
                start_page = i + 1
                if i >= about_total_page - partition_page:
                    end_page = None  # 最后一页，设为None
                else:
                    end_page = i + partition_page
                future = executor.submit(cls.collect_by_partition, start_page, end_page, search_date)
                future_list.append(future)

            total_data_list = []
            for r in future_list:
                data_list = r.result()
                if data_list:
                    total_data_list.extend(data_list)

            logger.info(f'采集华泰证券可充抵保证金证券数据共total_data_list:{len(total_data_list)}条')
            df_result = super().data_deal(total_data_list, data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds

            if int(len(total_data_list)) == db_total_count and int(len(total_data_list)) > 0 and db_total_count > 0:
                super().data_insert(int(len(total_data_list)), df_result, search_date,
                                    exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(total_data_list))}条')
            else:
                raise Exception(f'采集数据条数{int(len(total_data_list))}与官网数据条数{db_total_count}不一致，采集程序存在抖动，需要重新采集')

            message = "ht_securities_collect"
            super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                      exchange_mt_guaranty_security, data_source, message)

            logger.info("华泰证券可充抵保证金证券数据采集完成")

    @classmethod
    def collect_by_partition(cls, start_page, end_page, search_date):
        url = 'https://www.htsc.com.cn/browser/rzrqPool/getDbZqc.do'
        data_list = []
        hs_page = ss_page = start_page
        page_size = 8
        if end_page is None:
            end_page = 100000

        hs_is_continue = ss_is_continue = True
        retry_count = 5
        while (hs_is_continue or ss_is_continue) and hs_page <= end_page and ss_page <= end_page:
            params = {'date': search_date, 'hsPage': hs_page, 'hsPageSize': page_size, 'ssPage': ss_page,
                      'ssPageSize': page_size}
            logger.info("{}".format(params))
            try:
                proxies = get_proxies(3, 10)
                response = requests.post(url=url, params=params, proxies=proxies, timeout=6)
                text = json.loads(response.text)
                hs_data_list = text['result']['dbHs']
                ss_data_list = text['result']['dbSs']
            except Exception as e:
                logger.error(e)
                hs_is_continue = ss_is_continue = False
                if retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if hs_is_continue and len(hs_data_list) > 0:
                for i in hs_data_list:
                    market = '沪市'
                    stock_code = i['stockCode']
                    stock_name = i['stockName']
                    rate = i['assureRatio']
                    stock_group_name = i['stockGroupName']
                    data_list.append((market, stock_code, stock_name, rate, stock_group_name))

            if ss_is_continue and len(ss_data_list) > 0:
                for i in ss_data_list:
                    market = '深市'
                    stock_code = i['stockCode']
                    stock_name = i['stockName']
                    rate = i['assureRatio']
                    stock_group_name = i['stockGroupName']
                    data_list.append((market, stock_code, stock_name, rate, stock_group_name))

            if len(hs_data_list) == page_size:
                hs_page = hs_page + 1
            else:
                hs_is_continue = False

            if len(ss_data_list) == page_size:
                ss_page = ss_page + 1
            else:
                ss_is_continue = False

        return data_list


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(business_type=2, search_date='2022-07-18')
    # collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')
