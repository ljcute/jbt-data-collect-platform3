#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/24 13:33
import json
import traceback
import datetime
import pandas as pd
import xlrd2
import fire
from constants import USER_AGENTS
import requests
import random
import os
from data.dao import sz_data_deal, sh_data_deal
from utils.proxy_utils import get_proxies
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sz_balance.xlsx')

data_type_market_mt_trading_amount = '0'  # 市场融资融券交易总量
data_type_market_mt_trading_items = '1'  # 市场融资融券交易明细

data_source_szse = 'szse'
data_source_sse = 'sse'


def total_data_collect(query_date=None):
    url = 'http://www.szse.cn/api/report/ShowReport/data'
    headers = {
        'User-Agent': random.choice(USER_AGENTS)
    }
    params = {
        'SHOWTYPE': 'JSON',
        'CATALOGID': '1837_xxpl',
        'loading': 'first',
        # 查历史可以传日期
        'txtDate': query_date,
        'random': random_double()
    }

    try:
        data_list = []
        response = requests.get(url=url, proxies=get_proxies(), headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            start_dt = datetime.datetime.now()

            text = response.text
            loads = json.loads(text)
            zero_ele = loads[0]
            title_data = zero_ele['data'][0]
            subname = zero_ele['metadata']['subname']  # 业务数据的交易日期
            # 融资融券交易总量
            jrrzye = title_data['jrrzye']  # 融资余额(亿元)
            jrrjye = title_data['jrrjye']  # 融券余额(亿元)
            jrrzrjye = title_data['jrrzrjye']  # 融资融券余额(亿元)
            jrrzmr = title_data['jrrzmr']  # 融资买入额(亿元)
            jrrjmc = title_data['jrrjmc']  # 融券卖出量(亿股/亿份)
            jrrjyl = title_data['jrrjyl']  # 融券余量(亿股/亿份)
            data_list.append((jrrzye, jrrjye, jrrzrjye, jrrzmr, jrrjmc, jrrjyl))

            end_dt = datetime.datetime.now()
            # 计算采集数据所需时间used_time
            used_time = (end_dt - start_dt).seconds
            data_df = pd.DataFrame(data_list, columns=['jrrzye', 'jrrjye', 'jrrzrjye', 'jrrzmr', 'jrrjmc', 'jrrjyl'])
            print(data_df)
            if data_df is not None:
                if data_df.iloc[:, 0].size == len(data_list) and (str(subname).replace("-", "")) == str(query_date):
                    df_result = {
                        'columns': ['jrrzye', 'jrrjye', 'jrrzrjye', 'jrrzmr', 'jrrjmc', 'jrrjyl'],
                        'data': data_df.values.tolist()
                    }
                    sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), subname
                                                       , data_type_market_mt_trading_amount, data_source_szse, start_dt,
                                                       end_dt, used_time)

    except Exception as es:
        logger.error(es)


def download_excel(query_date=None):
    # url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1837_xxpl&txtDate=2022-04-26&random=0.9683071637992591&TABKEY=tab2'
    download_url = "https://www.szse.cn/api/report/ShowReport"
    headers = {
        'User-Agent': random.choice(USER_AGENTS)
    }
    params = {
        'SHOWTYPE': 'xlsx',
        'CATALOGID': '1837_xxpl',
        'txtDate': query_date,  # 查历史可以传日期
        'random': random_double(),
        'TABKEY': 'tab2'
    }

    try:
        response = requests.get(url=download_url, headers=headers, params=params, timeout=20)
        with open(excel_file_path, 'wb') as file:
            file.write(response.content)  # 写excel到当前目录
    except Exception as es:
        logger.error(es)


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


def handle_excel(excel_file, date, excel_file_path):
    start_dt = datetime.datetime.now()
    sheet_0 = excel_file.sheet_by_index(0)
    total_row = sheet_0.nrows

    try:
        data_list = []
        for i in range(1, total_row):  # 从第2行开始遍历
            row = sheet_0.row(i)
            if row is None:
                break

            zqdm = str(row[0].value).replace(",", "")  # 证券代码
            zqjc = str(row[1].value).replace(",", "")  # 证券简称
            jrrzye = float(str(row[3].value).replace(",", ""))  # 融资余额(元)
            jrrzmr = float(str(row[2].value).replace(",", ""))  # 融资买入额(元)
            jrrjyl = float(str(row[5].value).replace(",", ""))  # 融券余量(股/份)
            jrrjye = float(str(row[6].value).replace(",", ""))  # 融券余额(元)
            jrrjmc = float(str(row[4].value).replace(",", ""))  # 融券卖出量(股/份)
            jrrzrjye = float(str(row[7].value).replace(",", ""))  # 融资融券余额(亿元)
            data_list.append((zqdm, zqjc, jrrzye, jrrzmr, jrrjyl, jrrjye, jrrjmc, jrrzrjye))

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        data_df = pd.DataFrame(data_list,
                               columns=['zqdm', 'zqjc', 'jrrzye', 'jrrzmr', 'jrrjyl', 'jrrjye', 'jrrjmc', 'jrrzrjye'])
        print(data_df)
        if data_df is not None:
            if data_df.iloc[:, 0].size == total_row - 1:
                df_result = {
                    'columns': ['zqdm', 'zqjc', 'jrrzye', 'jrrzmr', 'jrrjyl', 'jrrjye', 'jrrjmc', 'jrrzrjye'],
                    'data': data_df.values.tolist()
                }
                sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), date
                                                   , data_type_market_mt_trading_items, data_source_szse, start_dt,
                                                   end_dt, used_time, excel_file_path)

    except Exception as es:
        logger.error(es)


def remove_file(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        logger.error("删除文件异常:{}".format(e))


def collect(query_date=None):
    try:
        actual_date = sz_data_deal.get_max_biz_dt() if query_date is None else query_date
        logger.info("深交所数据采集日期actual_date:{}".format(actual_date))
        total_data_collect(actual_date)
        download_excel(actual_date)
        excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
        handle_excel(excel_file, actual_date, excel_file_path)
    except Exception as es:
        traceback.format_exc()
        logger.error("es:{}", es)
    finally:
        remove_file(excel_file_path)


def collect_history(begin_dt, end_dt):
    # begin = datetime.datetime.strptime('20210605', '%Y%m%d')
    begin = datetime.datetime.strptime(begin_dt, '%Y%m%d')
    end = datetime.datetime.strptime(end_dt, '%Y%m%d')
    b = begin.date()
    e = end.date()

    for k in range((e - b).days + 1):
        cur_date = b + datetime.timedelta(days=k)
        collect(cur_date)


if __name__ == "__main__":
    # download_excel('2022-06-24')
    # excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
    # handle_excel(excel_file, '2022-04-26')
    # data_collect()
    # collect("2022-05-23")
    # collect_history('20220620', '20220621')
    # total_data_collect('2022-06-24')

    fire.Fire()

    # python3 sz_market_mt_trading_collect.py - collect_history 20220620 20220621
