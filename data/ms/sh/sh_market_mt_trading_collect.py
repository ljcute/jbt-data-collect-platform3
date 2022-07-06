#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/23 13:33
import json
import traceback
import datetime
import pandas as pd
import xlrd2
from constants import USER_AGENTS
import requests
import datetime
import fire
import random
import os
from data.dao import sh_data_deal
from utils.proxy_utils import get_proxies
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sh_balance.xls')

data_type_market_mt_trading_amount = '0'  # 市场融资融券交易总量
data_type_market_mt_trading_items = '1'  # 市场融资融券交易明细

data_source_szse = 'szse'
data_source_sse = 'sse'


def download_excel(query_date=None):
    download_excel_url = "http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk20220623.xls"
    if query_date is not None:
        replace_str = 'rzrqjygk' + str(query_date).format("'%Y%m%d'").replace('-', '') + '.xls'
        download_excel_url = download_excel_url.replace(download_excel_url.split('/')[-1], replace_str)
    print(download_excel_url)
    headers = {
        'User-Agent': random.choice(USER_AGENTS)
    }
    response = requests.get(url=download_excel_url, proxies=get_proxies(), headers=headers, timeout=20)
    if response.status_code == 200:
        try:
            with open(excel_file_path, 'wb') as file:
                file.write(response.content)  # 写excel到当前目录
        except Exception as es:
            print(es)
    else:
        logger.info("上交所该日无数据:txt_date:{}".format(query_date))


def handle_excel_total(excel_file, date, excel_file_path=None):
    start_dt = datetime.datetime.now()
    sheet_0 = excel_file.sheet_by_index(0)
    total_row = sheet_0.nrows
    try:
        data_list = []
        for i in range(1, 2):  # 从第2行开始遍历
            row = sheet_0.row(i)
            if row is None:
                break
            date = str(date).format("'%Y%m%d'").replace('-', '')  # 信用交易日期
            rzye = float(str(row[0].value).replace(",", ""))  # 融资余额（元）
            rzmre = float(str(row[1].value).replace(",", ""))  # 融资买入额（元）
            rjyl = str(row[2].value).replace(",", "")  # 融券余量
            rjylje = float(str(row[3].value).replace(",", ""))  # 融券余量金额(元)
            rjmcl = str(row[4].value).replace(",", "")  # 融券卖出量
            rzrjye = float(str(row[5].value).replace(",", ""))  # 融资融券余额(元)
            data_list.append((date, rzye, rzmre, rjyl, rjylje, rjmcl, rzrjye))

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        data_df = pd.DataFrame(data_list,
                               columns=['date', 'rzye', 'rzmre', 'rjyl', 'rjylje', 'rjmcl', 'rzrjye'])
        print(data_df)
        if data_df is not None:
            if data_df.iloc[:, 0].size == 1:
                df_result = {
                    'columns': ['date', 'rzye', 'rzmre', 'rjyl', 'rjylje', 'rjmcl', 'rzrjye'],
                    'data': data_df.values.tolist()
                }
                sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), date
                                                   , data_type_market_mt_trading_amount, data_source_sse, start_dt,
                                                   end_dt, used_time, excel_file_path)
        handle_excel_detail(excel_file, date, excel_file_path)

    except Exception as es:
        print(es)


def handle_excel_detail(excel_file, date, excel_file_path=None):
    start_dt = datetime.datetime.now()
    sheet_1 = excel_file.sheet_by_index(1)
    total_row = sheet_1.nrows
    try:
        data_list = []
        for i in range(1, total_row):  # 从第2行开始遍历
            row = sheet_1.row(i)
            if row is None:
                break
            date = str(date).format("'%Y%m%d'").replace('-', '')  # 信用交易日期
            bdzjdm = str(row[0].value).replace(",", "")  # 标的证劵代码
            bdzjjc = str(row[1].value).replace(",", "")  # 标的证劵简称
            rzye = float(str(row[2].value).replace(",", ""))  # 融资余额(元)
            rzmre = float(str(row[3].value).replace(",", ""))  # 融资买入额(元)
            rzche = float(str(row[4].value).replace(",", ""))  # 融资偿还额(元)
            rjyl = str(row[5].value).replace(",", "")  # 融券余量
            rjmcl = str(row[6].value).replace(",", "")  # 融券卖出量
            rjchl = str(row[7].value).replace(",", "")  # 融资偿还量
            data_list.append((date, bdzjdm, bdzjjc, rzye, rzmre, rzche, rjyl, rjmcl, rjchl))

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        data_df = pd.DataFrame(data_list,
                               columns=['date', 'bdzjdm', 'bdzjjc', 'rzye', 'rzmre', 'rzche', 'rjyl', 'rjmcl', 'rjchl'])
        print(data_df)
        if data_df is not None:
            if data_df.iloc[:, 0].size == total_row - 1:
                df_result = {
                    'columns': ['date', 'bdzjdm', 'bdzjjc', 'rzye', 'rzmre', 'rzche', 'rjyl', 'rjmcl', 'rjchl'],
                    'data': data_df.values.tolist()
                }
                sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), date
                                                   , data_type_market_mt_trading_items, data_source_sse, start_dt,
                                                   end_dt, used_time, excel_file_path)
    except Exception as es:
        print(es)


def remove_file(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        logger.info("删除文件异常:{}".format(e))


def collect(query_date=None):
    try:
        actual_date = sh_data_deal.get_max_biz_dt() if query_date is None else query_date
        logger.info("上交所数据采集日期actual_date:{}".format(actual_date))
        download_excel(actual_date)
        excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
        handle_excel_total(excel_file, actual_date, excel_file_path)
    except Exception as es:
        traceback.format_exc()
        logger.info("es:{}", es)
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
    # download_excel()
    # data_collect()
    # collect("2022-05-23")
    # collect_history('20220620', '20220623')
    # query_date = '2022-06-02'
    # download_excel_url = "http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk20220623.xls"
    # replace_str = 'rzrqjygk' + str(query_date).format("'%Y%m%d'").replace('-', '') + '.xls'
    # download_excel_url = download_excel_url.replace(download_excel_url.split('/')[-1], replace_str)
    # print(download_excel_url)
    # download_excel('20220601')
    # excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
    # handle_excel_total(excel_file, '2022-06-01', excel_file_path)
    # collect('20220606')
    # collect_history('20220621', '20220624')
    fire.Fire()

    # python3 sh_market_mt_trading_collect.py - collect_history 20220621 20220624
