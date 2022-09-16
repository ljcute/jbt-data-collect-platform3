#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/19 13:14
# 国泰君安
import os
import sys
import concurrent.futures
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)


from utils.exceptions_utils import ProxyTimeOutEx
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

data_source = '国泰君安证券'
url_ = 'https://www.gtja.com/cos/rest/margin/path/fuzzy.json'

class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type, search_date=None):
        search_date = search_date if search_date is not None else datetime.date.today()
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            if business_type:
                if business_type == 4:
                    try:
                        # 国泰君安证券融资标的证券采集
                        cls.rz_target_collect(search_date)
                        break
                    except ProxyTimeOutEx as es:
                        pass
                    except Exception as e:
                        logger.error(f'{data_source}融资标的证券采集任务异常，请求url为：{url_}，具体异常信息为：{traceback.format_exc()}')
                elif business_type == 5:
                    try:
                        # 国泰君安证券融券标的证券采集
                        cls.rq_target_collect(search_date)
                        break
                    except ProxyTimeOutEx as es:
                        pass
                    except Exception as e:
                        logger.error(f'{data_source}融券标的证券采集任务异常，请求url为：{url_}，具体异常信息为：{traceback.format_exc()}')
                elif business_type == 2:
                    try:
                        # 国泰君安证券可充抵保证金采集
                        cls.guaranty_collect(search_date)
                        break
                    except ProxyTimeOutEx as es:
                        pass
                    except Exception as e:
                        logger.error(f'{data_source}可充抵保证金证券采集任务异常，请求url为：{url_}，具体异常信息为：{traceback.format_exc()}')

            max_retry += 1

    @classmethod
    def rz_target_collect(cls, search_date):
        logger.info(f'开始采集国泰君安证券融资标的证券数据{search_date}')
        url = 'https://www.gtja.com/cos/rest/margin/path/fuzzy.json'
        params = {"pageNum": 1, "type": 3, "_": remove_file.get_timestamp(), "stamp": search_date}  # type=3表示融资
        start_dt = datetime.datetime.now()
        try:
            proxies = super().get_proxies()
            response = super().get_response(data_source, url, proxies, 0, get_headers(), params, None,
                                            allow_redirects=False)
            if response is None or response.status_code != 200:
                raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
            total = None
            if response.status_code == 200:
                text = json.loads(response.text)
                total = int(text['total'])
            else:
                logger.error(f'请求失败，respones.status={response.status_code}')
                raise Exception(f'请求失败，respones.status={response.status_code}')

            data_title = ['stock_code', 'stock_name', 'rate', 'branch']

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                future_list = []
                about_total_page = 300
                partition_page = 50
                for i in range(0, about_total_page, partition_page):
                    start_page = i + 1
                    if i >= about_total_page - partition_page:
                        end_page = None  # 最后一页
                    else:
                        end_page = i + partition_page

                    logger.info(f'这是第{i}此循环')
                    future = executor.submit(cls.collect_by_partition_rz, start_page, end_page, search_date)
                    future_list.append(future)

                total_data_list = []
                for r in future_list:
                    data_list = r.result()
                    if data_list:
                        total_data_list.extend(data_list)

                logger.info(f'采集国泰君安证券融资标的证券数据共total_data_list:{len(total_data_list)}条')
                df_result = super().data_deal(total_data_list, data_title)
                end_dt = datetime.datetime.now()
                used_time = (end_dt - start_dt).seconds
                if int(len(total_data_list)) == total and int(len(total_data_list)) > 0 and total > 0:
                    data_status = 1
                    super().data_insert(int(len(total_data_list)), df_result, search_date,
                                        exchange_mt_financing_underlying_security,
                                        data_source, start_dt, end_dt, used_time, url, data_status)
                    logger.info(f'入库信息,共{int(len(total_data_list))}条')
                elif int(len(total_data_list)) != total:
                    data_status = 2
                    super().data_insert(int(len(total_data_list)), df_result, search_date,
                                        exchange_mt_financing_underlying_security,
                                        data_source, start_dt, end_dt, used_time, url, data_status)
                    logger.info(f'入库信息,共{int(len(total_data_list))}条')

                message = "gtja_securities_collect"
                super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                          exchange_mt_financing_underlying_security, data_source, message)

                logger.info("国泰君安证券融资标的证券数据采集完成")
        except Exception as e:
            data_status = 2
            super().data_insert(0, str(e), search_date, exchange_mt_financing_underlying_security,
                                data_source, start_dt, None, None, url, data_status)

            raise Exception(e)

    @classmethod
    def collect_by_partition_rz(cls, start_page, end_page, search_date):
        logger.info(f'进入此线程{start_page}')
        url = 'https://www.gtja.com/cos/rest/margin/path/fuzzy.json'
        data_list = []
        if end_page is None:
            end_page = 100000

        is_continue = True
        proxies = super().get_proxies()
        retry_count = 5
        while is_continue and start_page <= end_page:
            params = {"pageNum": start_page, "type": 3, "_": remove_file.get_timestamp(),
                      "stamp": search_date}  # type=3表示融资
            try:
                response = super().get_response(data_source, url, proxies, 0, get_headers(), params, None, allow_redirects=False)
                if response is None or response.status_code != 200:
                    raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
                text = json.loads(response.text)
                finance_list = text['finance']
            except Exception as e:
                logger.error(e)
                is_continue = False
                if retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if is_continue and len(finance_list) > 0:
                for i in finance_list:
                    stock_code = i['secCode']
                    stock_name = i['secAbbr']
                    rate = i['rate']
                    branch = i['branch']
                    data_list.append((stock_code, stock_name, rate, branch))

            if int(len(finance_list)) == 10:
                start_page = start_page + 1
            else:
                is_continue = False

        logger.info(f'此线程采集数据共datalist:{len(data_list)}条')
        return data_list

    @classmethod
    def rq_target_collect(cls, search_date):
        logger.info(f'开始采集国泰君安证券融券标的证券数据{search_date}')
        url = 'https://www.gtja.com/cos/rest/margin/path/fuzzy.json'
        params = {"pageNum": 1, "type": 2, "_": remove_file.get_timestamp(), "stamp": search_date}  # type=3表示融资
        start_dt = datetime.datetime.now()
        try:
            proxies = super().get_proxies()
            response = super().get_response(data_source, url, proxies, 0, get_headers(), params, None,
                                            allow_redirects=False)
            if response is None or response.status_code != 200:
                raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
            total = None
            if response.status_code == 200:
                text = json.loads(response.text)
                total = int(text['total'])
            else:
                logger.error(f'请求失败，respones.status={response.status_code}')
                raise Exception(f'请求失败，respones.status={response.status_code}')
            data_title = ['stock_code', 'stock_name', 'rate', 'branch']

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                future_list = []
                about_total_page = 300
                partition_page = 50
                for i in range(0, about_total_page, partition_page):
                    start_page = i + 1
                    if i >= about_total_page - partition_page:
                        end_page = None  # 最后一页
                    else:
                        end_page = i + partition_page

                    logger.info(f'这是第{i}此循环')
                    future = executor.submit(cls.collect_by_partition_rq, start_page, end_page, search_date)
                    future_list.append(future)

                total_data_list = []
                for r in future_list:
                    data_list = r.result()
                    if data_list:
                        total_data_list.extend(data_list)

                logger.info(f'采集国泰君安证券融券标的证券数据共total_data_list:{len(total_data_list)}条')
                df_result = super().data_deal(total_data_list, data_title)
                end_dt = datetime.datetime.now()
                used_time = (end_dt - start_dt).seconds
                if int(len(total_data_list)) == total and int(len(total_data_list)) > 0 and total > 0:
                    data_status = 1
                    super().data_insert(int(len(total_data_list)), df_result, search_date,
                                        exchange_mt_lending_underlying_security,
                                        data_source, start_dt, end_dt, used_time, url, data_status)
                    logger.info(f'入库信息,共{int(len(total_data_list))}条')
                elif int(len(total_data_list)) != total:
                    data_status = 2
                    super().data_insert(int(len(total_data_list)), df_result, search_date,
                                        exchange_mt_lending_underlying_security,
                                        data_source, start_dt, end_dt, used_time, url, data_status)
                    logger.info(f'入库信息,共{int(len(total_data_list))}条')

                message = "gtja_securities_collect"
                super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                          exchange_mt_lending_underlying_security, data_source, message)

                logger.info("国泰君安证券融券标的证券数据采集完成")
        except Exception as e:
            data_status = 2
            super().data_insert(0, str(e), search_date, exchange_mt_lending_underlying_security,
                                data_source, start_dt, None, None, url, data_status)

            raise Exception(e)

    @classmethod
    def collect_by_partition_rq(cls, start_page, end_page, search_date):
        logger.info(f'进入此线程{start_page}')
        url = 'https://www.gtja.com/cos/rest/margin/path/fuzzy.json'
        data_list = []
        if end_page is None:
            end_page = 100000

        is_continue = True
        proxies = super().get_proxies()
        retry_count = 5
        while is_continue and start_page <= end_page:
            params = {"pageNum": start_page, "type": 2, "_": remove_file.get_timestamp(),
                      "stamp": search_date}  # type=3表示融资
            try:
                response = super().get_response(data_source, url, proxies, 0, get_headers(), params, None, allow_redirects=False)
                if response is None or response.status_code != 200:
                    raise Exception(f'{data_source}数据采集任务请求响应获取异常,第{start_page}页无成功响应，具体采集数目未知，已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
                text = json.loads(response.text)
                security_list = text['security']
            except Exception as e:
                logger.error(e)
                is_continue = False
                if retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if is_continue and len(security_list) > 0:
                for i in security_list:
                    stock_code = i['secCode']
                    stock_name = i['secAbbr']
                    rate = i['rate']
                    branch = i['branch']
                    data_list.append((stock_code, stock_name, rate, branch))

            if int(len(security_list)) == 10:
                start_page = start_page + 1
            else:
                is_continue = False

        logger.info(f'此线程采集数据共datalist:{len(data_list)}条')
        return data_list

    @classmethod
    def guaranty_collect(cls, search_date):
        logger.info(f'开始采集国泰君安证券可充抵保证金证券数据{search_date}')
        url = 'https://www.gtja.com/cos/rest/margin/path/fuzzy.json'
        params = {"pageNum": 1, "type": 1, "_": remove_file.get_timestamp(), "stamp": search_date}  # type=3表示融资
        start_dt = datetime.datetime.now()
        try:
            proxies = super().get_proxies()
            response = super().get_response(data_source, url, proxies, 0, get_headers(), params, None,
                                            allow_redirects=False)
            if response is None or response.status_code != 200:
                raise Exception(
                    f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
            total = None
            if response.status_code == 200:
                text = json.loads(response.text)
                total = int(text['total'])
            else:
                logger.error(f'请求失败，respones.status={response.status_code}')
                raise Exception(f'请求失败，respones.status={response.status_code}')
            data_title = ['stock_code', 'stock_name', 'rate', 'branch']

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_list = []
                about_total_page = 1600
                partition_page = 200
                for i in range(0, about_total_page, partition_page):
                    start_page = i + 1
                    if i >= about_total_page - partition_page:
                        end_page = None  # 最后一页
                    else:
                        end_page = i + partition_page

                    logger.info(f'这是第{i}此循环')
                    future = executor.submit(cls.collect_by_partition_bzj, start_page, end_page, search_date)
                    future_list.append(future)

                total_data_list = []
                for r in future_list:
                    data_list = r.result()
                    if data_list:
                        total_data_list.extend(data_list)

                logger.info(f'采集国泰君安证券可充抵保证金证券数据共total_data_list:{len(total_data_list)}条')
                df_result = super().data_deal(total_data_list, data_title)
                end_dt = datetime.datetime.now()
                used_time = (end_dt - start_dt).seconds
                if int(len(total_data_list)) == total and int(len(total_data_list)) > 0 and total > 0:
                    data_status = 1
                    super().data_insert(int(len(total_data_list)), df_result, search_date,
                                        exchange_mt_guaranty_security,
                                        data_source, start_dt, end_dt, used_time, url, data_status)
                    logger.info(f'入库信息,共{int(len(total_data_list))}条')
                elif int(len(total_data_list)) == total:
                    data_status = 2
                    super().data_insert(int(len(total_data_list)), df_result, search_date,
                                        exchange_mt_guaranty_security,
                                        data_source, start_dt, end_dt, used_time, url, data_status)
                    logger.info(f'入库信息,共{int(len(total_data_list))}条')

                message = "gtja_securities_collect"
                super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                          exchange_mt_guaranty_security, data_source, message)

                logger.info("国泰君安证券可充抵保证金证券数据采集完成")
        except Exception as e:
            data_status = 2
            super().data_insert(0, str(e), search_date, exchange_mt_guaranty_security,
                                data_source, start_dt, None, None, url, data_status)

            raise Exception(e)

    @classmethod
    def collect_by_partition_bzj(cls, start_page, end_page, search_date):
        logger.info(f'进入此线程{start_page}')
        url = 'https://www.gtja.com/cos/rest/margin/path/fuzzy.json'
        data_list = []
        if end_page is None:
            end_page = 100000

        is_continue = True
        proxies = super().get_proxies()
        retry_count = 5
        while is_continue and start_page <= end_page:
            params = {"pageNum": start_page, "type": 1, "_": remove_file.get_timestamp(),
                      "stamp": search_date}  # type=3表示融资
            try:
                response = super().get_response(data_source, url, proxies, 0, get_headers(), params, None, allow_redirects=False)
                if response is None or response.status_code != 200:
                    raise Exception(f'{data_source}数据采集任务请求响应获取异常,第{start_page}页无成功响应，具体采集数目未知，已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')
                text = json.loads(response.text)
                offset_list = text['offset']
            except Exception as e:
                logger.error(e)
                is_continue = False
                if retry_count > 0:
                    retry_count = retry_count -1
                    time.sleep(5)
                    continue

            if is_continue and len(offset_list) > 0:
                for i in offset_list:
                    stock_code = i['secCode']
                    stock_name = i['secAbbr']
                    rate = i['rate']
                    branch = i['branch']
                    data_list.append((stock_code, stock_name, rate, branch))

            if int(len(offset_list)) == 10:
                start_page = start_page + 1
            else:
                is_continue = False

        logger.info(f'此线程采集数据共datalist:{len(data_list)}条')
        return data_list


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(search_date='2022-07-18')
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')