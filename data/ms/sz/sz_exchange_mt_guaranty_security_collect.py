#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/27 09:33

import json
import traceback
import pandas as pd
import xlrd2
from constants import USER_AGENTS
import requests
import random
import os
import fire
import datetime
from data.dao import sz_data_deal, sh_data_deal
import re
from utils.proxy_utils import get_proxies
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sz_exchange_mt_guaranty_security.xlsx')

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '3.1'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '3.2'  # 融资融券融券标的证券

data_source_szse = 'szse'
data_source_sse = 'sse'

regrex_pattern = re.compile(r"[(](.*?)[)]", re.S)  # 最小匹配,提取括号内容
regrex_pattern2 = re.compile(r"[(](.*)[)]", re.S)  # 贪婪匹配


def download_excel(query_date=None):
    download_url = 'http://www.szse.cn/api/report/ShowReport'
    headers = {
        'User-Agent': random.choice(USER_AGENTS)
    }
    params = {
        'SHOWTYPE': 'xlsx',
        'CATALOGID': '1835_xxpl',
        'TABKEY': 'tab1',
        # 查历史可以传日期
        'txtDate': query_date,
        'random': random_double()
    }

    try:
        response = requests.get(url=download_url, proxies=get_proxies(), headers=headers, params=params, timeout=20)
        with open(excel_file_path, 'wb') as file:
            file.write(response.content)  # 写excel到当前目录
    except Exception as es:
        print(es)


def handle_excel(excel_file, date, excel_file_path):
    start_dt = datetime.datetime.now()
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
                data_list.append((zqdm, zqjc))

            end_dt = datetime.datetime.now()
            # 计算采集数据所需时间used_time
            used_time = (end_dt - start_dt).seconds
            data_df = pd.DataFrame(data_list, columns=['zqdm', 'zqjc'])
            print(data_df)
            if data_df is not None:
                if data_df.iloc[:, 0].size == total_row - 1:
                    df_result = {
                        'columns': ['zqdm', 'zqjc'],
                        'data': data_df.values.tolist()
                    }
                    sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), date
                                                       , exchange_mt_guaranty_security, data_source_szse, start_dt,
                                                       end_dt, used_time, excel_file_path)
        else:
            logger.info("深交所该日无数据:txt_date:{}".format(date))

    except Exception as es:
        logger.error(es)


def remove_file(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        logger.info("删除文件异常:{}".format(e))


def random_double(mu=0.8999999999999999, sigma=0.1000000000000001):
    """
        访问深交所时需要随机数
        :param mu:
        :param sigma:
        :return:
    """
    random_value = random.normalvariate(mu, sigma)
    if random_value < 0:
        random_value = mu
    return random_value


def exchange_mt_guaranty_security_collect(query_date=None, query_end_data=None):
    if query_end_data is None:
        query_date = datetime.datetime.strptime(str(query_date).replace("-", ""), '%Y%m%d').date()
        try:
            actual_date = sz_data_deal.get_max_biz_dt() if query_date is None else query_date
            logger.info("深交所数据采集日期actual_date:{}".format(actual_date))
            download_excel(actual_date)
            excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
            handle_excel(excel_file, actual_date, excel_file_path)
        except Exception as es:
            traceback.format_exc()
            logger.info("es:{}", es)
        finally:
            remove_file(excel_file_path)
    else:
        try:
            begin = datetime.datetime.strptime(query_date, '%Y%m%d')
            end = datetime.datetime.strptime(query_end_data, '%Y%m%d')
            b = begin.date()
            e = end.date()

            for k in range((e - b).days + 1):
                cur_date = b + datetime.timedelta(days=k)
                exchange_mt_guaranty_security_collect(query_date=cur_date)
        except Exception as es:
            traceback.format_exc()
            logger.info("es:{}", es)
        finally:
            remove_file(excel_file_path)


if __name__ == '__main__':
    # exchange_mt_guaranty_security_collect('2022-06-27')
    # download_excel('2022-06-27')
    # excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
    # handle_excel(excel_file, '2022-06-27', excel_file_path)
    # exchange_mt_guaranty_security_collect('20220613', '20220615')
    fire.Fire()

    # python3 sz_exchange_mt_guaranty_security_collect.py - exchange_mt_guaranty_security_collect 20220613 20220615