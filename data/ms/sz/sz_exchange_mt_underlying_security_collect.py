#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/27 09:33
# 深圳交易所-融资融券标的证券

import os
import sys
import time
from configparser import ConfigParser



BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder
from utils.remove_file import remove_file, random_double

import json
import xlrd2
from constants import USER_AGENTS
import random
import os
import datetime
import re
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sz_exchange_mt_underlying_security.xlsx')

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path,encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, '深交所标的券.xlsx')

data_source_szse = '深圳交易所'
data_source_sse = '上海交易所'
broker_id = 1000094

regrex_pattern = re.compile(r"[(](.*?)[)]", re.S)  # 最小匹配,提取括号内容
regrex_pattern2 = re.compile(r"[(](.*)[)]", re.S)  # 贪婪匹配


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, query_date=None):
        max_retry = 0
        while max_retry < 3:
            try:
                actual_date = datetime.date.today() if query_date is None else query_date
                logger.info("深交所数据采集日期actual_date:{}".format(actual_date))
                download_url = 'http://www.szse.cn/api/report/ShowReport'
                headers = {
                    'User-Agent': random.choice(USER_AGENTS)
                }
                params = {
                    'SHOWTYPE': 'xlsx',
                    'CATALOGID': '1834_xxpl',
                    # 查历史可以传日期
                    'txtDate': actual_date,
                    'tab1PAGENO': 1,
                    'random': random_double(),
                    'TABKEY': 'tab1',
                }
                proxies = super().get_proxies()
                title_list = ['zqdm', 'zqjc', 'rzbd', 'rqbd', 'drkrz', 'drkrq', 'rqmcjgxz', 'zdfxz']

                start_dt = datetime.datetime.now()
                response = super().get_response(download_url, proxies, 0, headers, params)
                logger.info("开始下载excel")
                cls.download_excel(response)
                logger.info("excel下载完成")
                excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
                data_list, total_row = cls.handle_excel(excel_file, actual_date)
                df_result = super().data_deal(data_list, title_list)
                end_dt = datetime.datetime.now()
                used_time = (end_dt - start_dt).seconds
                if int(len(data_list)) == total_row - 1:
                    super().data_insert(int(len(data_list)), df_result, actual_date, exchange_mt_underlying_security,
                                        data_source_szse, start_dt, end_dt, used_time, download_url,
                                        save_excel_file_path)
                    logger.info(f'入库信息,共{int(len(data_list))}条')
                else:
                    raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{total_row - 1}不一致，入库失败')

                message = "深交所融资融券标的证券数据采集完成"
                super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                          exchange_mt_underlying_security, data_source_szse, message)

                logger.info("深交所融资融券标的证券数据采集完成")

                break
            except Exception as e:
                time.sleep(3)
                logger.error(e)
            finally:
                remove_file(excel_file_path)

            max_retry += 1

    @classmethod
    def download_excel(cls, response):
        try:
            with open(excel_file_path, 'wb') as file:
                file.write(response.content)
            with open(save_excel_file_path, 'wb') as file:
                file.write(response.content)
        except Exception as es:
            logger.error(es)

    @classmethod
    def handle_excel(cls, excel_file, actual_date):
        logger.info("开始处理excel")
        sheet_0 = excel_file.sheet_by_index(0)
        total_row = sheet_0.nrows

        try:
            if total_row > 1:
                data_list = []
                for i in range(1, total_row):
                    row = sheet_0.row(i)
                    if row is None:
                        break

                    zqdm = str(row[0].value)  # 证券代码
                    zqjc = str(row[1].value)  # 证券简称
                    rzbd = str(row[2].value)  # 融资标的
                    rqbd = str(row[3].value)  # 融券标的
                    drkrz = str(row[4].value)  # 当日可融资
                    drkrq = str(row[5].value)  # 当日可融券
                    rqmcjgxz = str(row[6].value)  # 融券卖出价格限制
                    zdfxz = str(row[7].value)  # 涨跌幅限制
                    data_list.append((zqdm, zqjc, rzbd, rqbd, drkrz, drkrq, rqmcjgxz, zdfxz))

                logger.info(f'已采集数据条数：{total_row - 1}')
                return data_list, total_row
            else:
                logger.info("深交所该日无数据:txt_date:{}".format(actual_date))
        except Exception as es:
            logger.error(es)


if __name__ == '__main__':
    collector = CollectHandler()
    if len(sys.argv) > 1:
        collector.collect_data(sys.argv[1])
    else:
        collector.collect_data()