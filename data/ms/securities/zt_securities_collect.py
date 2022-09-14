#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/20 15:42
# 中泰证券
import concurrent.futures
import os
import sys


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)


from utils.exceptions_utils import ProxyTimeOutEx
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

data_source = '中泰证券'


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
                        # 中泰证券标的证券采集
                        cls.target_collect(search_date)
                    elif business_type == 2:
                        # 中泰证券可充抵保证金采集
                        cls.guaranty_collect(search_date)
                    else:
                        logger.error(f'business_type{business_type}输入有误，请检查！')

                break
            except ProxyTimeOutEx as es:
                pass
            except Exception as e:
                time.sleep(3)
                # logger.error(e)

            max_retry += 1

    @classmethod
    def target_collect(cls, search_date):
        logger.info(f'开始采集中泰证券标的证券数据{search_date}')
        url = 'https://www.95538.cn/rzrq/data/Handler.ashx'
        page = 1
        is_continue = True

        data_list = []
        data_title = ['stock_code', 'stock_name', 'rz_rate', 'rq_rate', 'status', 'date', 'note']
        params = {"action": "GetBdstockNoticesPager", "pageindex": page, "date": date_to_stamp(search_date)}
        headers = get_headers()
        headers['Referer'] = 'https://www.95538.cn/rzrq/rzrq1.aspx?action=GetEarnestmoneyNoticesPager&tab=3&keyword='
        start_dt = datetime.datetime.now()
        proxies = super().get_proxies()
        response = super().get_response(url, proxies, 0, headers, params)
        # 请求失败。重试三次
        retry_count = 3
        if response is None:
            while retry_count > 0:
                response = super().get_response(url, proxies, 0, headers, params)
                if response is not None:
                    break
                else:
                    retry_count = retry_count - 1
                    continue

        if response.status_code == 200:
            text = json.loads(response.text)
            total = int(text['PageTotal'])
            for curr_page in range(1, total + 1):
                logger.info(f'当前为第{curr_page}页')
                params = {"action": "GetBdstockNoticesPager", "pageindex": curr_page,
                          "date": date_to_stamp(search_date)}
                response = super().get_response(url, proxies, 0, headers, params)
                if response is None or response.status_code != 200:
                    logger.error(f'{data_source}请求失败,无成功请求响应，采集总记录数未知。。。')
                    raise Exception(f'{data_source}请求失败,无成功请求响应，采集总记录数未知。。。')
                if response.status_code == 200:
                    text = json.loads(response.text)
                    all_data_list = text['Items']
                    if all_data_list:
                        for i in all_data_list:
                            stock_code = i['STOCK_CODE']
                            stock_name = i['STOCK_NAME']
                            rz_rate = i['FUND_RATIOS']
                            rq_rate = i['STOCK_RATIOS']
                            status = i['STOCK_STATE']
                            date = search_date
                            note = i['NOTE']
                            data_list.append((stock_code, stock_name, rz_rate, rq_rate, status, date, note))
                            logger.info(f'已采集数据条数为：{int(len(data_list))}')
                else:
                    logger.error(f'请求失败，respones.status={response.status_code}')
                    raise Exception(f'请求失败，respones.status={response.status_code}')

            logger.info(f'采集中泰证券标的证券数据共{int(len(data_list))}条')
            df_result = super().data_deal(data_list, data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if int(len(data_list)) == int(len(df_result['data'])) and int(len(data_list)) > 0 and int(
                    len(df_result['data'])) > 0:
                data_status = 1
                super().data_insert(int(len(data_list)), df_result, search_date,
                                    exchange_mt_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(data_list))}条')
            elif int(len(data_list)) != int(len(df_result['data'])):
                logger.error(f'采集数据条数{int(len(data_list))}与官网数据条数{total}不一致，采集程序存在抖动，需要重新采集')
                data_status = 2
                super().data_insert(int(len(data_list)), df_result, search_date,
                                    exchange_mt_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(data_list))}条')

            message = "zt_securities_collect"
            super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                      exchange_mt_underlying_security, data_source, message)

            logger.info("中泰证券标的证券数据采集完成")
        else:
            logger.error(f'请求失败，respones.status={response.status_code}')
            raise Exception(f'请求失败，respones.status={response.status_code}')

    @classmethod
    def guaranty_collect(cls, search_date):
        logger.info(f'开始采集中泰证券可充抵保证金证券数据{search_date}')
        url = 'https://www.95538.cn/rzrq/data/Handler.ashx'
        data_title = ['stock_code', 'stock_name', 'rate', 'date', 'note']
        start_dt = datetime.datetime.now()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_list = []
            about_total_page = 2500
            partition_page = 250
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

            logger.info(f'采集中泰证券可充抵保证金证券数据共total_data_list:{len(total_data_list)}条')
            df_result = super().data_deal(total_data_list, data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            total = int(len(df_result['data']))
            if int(len(total_data_list)) == int(len(df_result['data'])) and int(len(total_data_list)) > 0 and int(
                    len(df_result['data'])) > 0:
                data_status = 1
                super().data_insert(int(len(total_data_list)), df_result, json.dumps(search_date),
                                    exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(total_data_list))}条')
            elif int(len(total_data_list)) != int(len(df_result['data'])):
                logger.error(f'采集数据条数{int(len(total_data_list))}与官网数据条数{total}不一致，采集程序存在抖动，需要重新采集')
                data_status = 2
                super().data_insert(int(len(total_data_list)), df_result, json.dumps(search_date),
                                    exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(total_data_list))}条')

            message = "zt_securities_collect"
            super().kafka_mq_producer(json.dumps(search_date, cls=ComplexEncoder),
                                      exchange_mt_guaranty_security, data_source, message)

            logger.info("中泰证券可充抵保证金证券数据采集完成")


    @classmethod
    def collect_by_partition_rz(cls, start_page, end_page, search_date):
        logger.info(f'进入此线程{start_page}')
        url = 'https://www.95538.cn/rzrq/data/Handler.ashx'
        data_list = []
        all_data_list = []
        if end_page is None:
            end_page = 100000

        is_continue = True
        retry_count = 3
        proxies = super().get_proxies()
        while is_continue and start_page <= end_page:
            params = {"action": "GetEarnestmoneyNoticesPager", "pageindex": start_page, "date": date_to_stamp(search_date)}
            headers = get_headers()
            headers['Referer'] = 'https://www.95538.cn/rzrq/rzrq1.aspx?action=GetEarnestmoneyNoticesPager&tab=3&keyword='

            try:
                response = super().get_response(url, proxies, 0, headers, params)
                if response is None or response.status_code != 200:
                    logger.error(f'{data_source}请求失败,无成功请求响应，采集总记录数未知。。。')
                    raise Exception(f'{data_source}请求失败,无成功请求响应，采集总记录数未知。。。')
                text = json.loads(response.text)
                all_data_list = text['Items']
            except Exception as e:
                # logger.error(e)
                if retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue
                else:
                    is_continue = False

            if is_continue and len(all_data_list) > 0:
                for i in all_data_list:
                    stock_code = i['STOCK_CODE']
                    stock_name = i['STOCK_NAME']
                    rate = i['REBATE']
                    date = search_date
                    note = i['NOTE']
                    data_list.append((stock_code, stock_name, rate, date, note))

            if int(len(all_data_list)) == 10:
                start_page = start_page + 1
            else:
                is_continue = False

        logger.info(f'此线程采集数据共datalist:{len(data_list)}条')
        return data_list


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(3, '2022-07-08')
    # collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')
