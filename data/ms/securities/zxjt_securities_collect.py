#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/29 16:47
# 中信建投

import os
import sys
from configparser import ConfigParser

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder

import json
import os
import time
import urllib.request
import xlrd2
from bs4 import BeautifulSoup
from constants import *
from utils.logs_utils import logger
import datetime

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '中信建投'

broker_id = 10006
guaranty_file_path = './' + str(broker_id) + 'guaranty.xls'
target_file_path = './' + str(broker_id) + 'target.xls'
all_file_path = './' + str(broker_id) + 'all.xls'

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, "中信建投证券三种数据整合{}.xls".format(datetime.date.today()))


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type):
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            try:
                if business_type:
                    if business_type == 99:
                        cls.all_collect()
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
    def all_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集中信建投数据{actual_date}')
        url = 'https://www.csc108.com/kyrz/rzrqList.json'
        start_dt = datetime.datetime.now()
        proxies = super().get_proxies()
        params = {'curPage': 1}
        response = super().get_response(url, proxies, 0, get_headers(), params)
        if response is None or response.status_code != 200:
            raise Exception(f'{data_source}数据采集任务获取请求失败,无成功请求响应,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{params}')

        target_title = ['sec_code', 'market', 'sec_name', 'bzj_type', 'bzj_rate', 'rz_rate', 'rq_rate', 'rz_flag',
                        'rq_flag']
        target_list = []
        text = json.loads(response.text)
        total = text['totalCount']
        data_list = text['list']
        for i in data_list:
            sec_code = i['stkCode']
            market = '深A' if i['market'] == '0' else '沪A'
            sec_name = i['stkName']
            bzj_type = i['type']
            if i['type'] == '2':
                bzj_type = '2'
            elif i['type'] is None:
                bzj_type = '1'
            elif i['type'] == '3':
                bzj_type = '3'
            bzj_rate = '-' if i['pledgerate'] is None else i['pledgerate']
            rz_rate = '-' if i['marginratefund'] is None else i['marginratefund']
            rq_rate = '-' if i['marginratestk'] is None else i['marginratestk']
            rz_flag = '-' if i['fundctrlflag'] is None else i['fundctrlflag']
            rq_flag = '-' if i['stkctrlflag'] is None else i['stkctrlflag']
            target_list.append((sec_code, market, sec_name, bzj_type, bzj_rate, rz_rate, rq_rate, rz_flag, rq_flag))

        print(target_list)
        logger.info(f'采集中信建投数据结束共{int(len(target_list))}条')
        df_result = super().data_deal(target_list, target_title)
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        if int(len(target_list)) == int(total) and int(len(target_list)) > 0 and int(total) > 0:
            data_status = 1
            super().data_insert(int(len(target_list)), df_result, actual_date,
                                exchange_mt_guaranty_and_underlying_security,
                                data_source, start_dt, end_dt, used_time, url, data_status)
            logger.info(f'入库完成,共{int(len(target_list))}条')
        elif int(len(target_list)) != int(total):
            logger.error(f'本次采集数据条数：{int(len(target_list))}，与官网数据条数：{int(total)}不一致，采集存在抖动，需重新采集！')
            data_status = 2
            super().data_insert(int(len(target_list)), df_result, actual_date,
                                exchange_mt_guaranty_and_underlying_security,
                                data_source, start_dt, end_dt, used_time, url, data_status)
            logger.info(f'入库完成,共{int(len(target_list))}条')

        message = "zxjt_securities_collect"
        super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                  exchange_mt_guaranty_and_underlying_security, data_source, message)

        logger.info("中信建投数据采集完成")


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(99)
    collector.collect_data(eval(sys.argv[1]))
