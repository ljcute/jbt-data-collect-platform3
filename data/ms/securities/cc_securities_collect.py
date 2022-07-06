#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/07/01 13:19
# 长城证券
import json
import time
import pandas as pd
from constants import *
from data.dao import sh_data_deal
from utils.logs_utils import logger
import fire
import datetime

# 定义常量
broker_id = 10011

cc_headers = {
    'Connection': 'close',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
}

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '3.1'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '3.2'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = 'cc_securities'


# 长城证券融资标的券采集
def rz_target_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集融资数据".format(broker_id))
    url = 'http://www.cgws.com/was5/web/de.jsp'
    page = 1
    page_size = 5
    is_continue = True
    data_list = []
    data_title = ['sec_code', 'sec_name', 'round_rate', 'date']
    start_dt = datetime.datetime.now()
    while is_continue:
        params = {"page": page, "channelid": 257420, "searchword": 'KGNyZWRpdGZ1bmRjdHJsPTAp',
                  "_": get_timestamp()}
        # logger.info("{}".format(params))
        try:
            response = requests.get(url=url, params=params, headers=cc_headers, timeout=10)
            text = json.loads(response.text)
            row_list = text['rows']
            total = 0
            if row_list:
                total = int(text['total'])
            if total is not None and type(total) is not str and total > page * page_size:
                is_continue = True
                page = page + 1
            else:
                is_continue = False
            for i in row_list:
                sec_code = i['code']
                if sec_code == "":  # 一页数据,遇到{"code":"","name":"","rate":"","pub_date":"","market":""}表示完结
                    break
                u_name = i['name'].replace('u', '\\u')  # 把u4fe1u7acbu6cf0转成\\u4fe1\\u7acb\\u6cf0
                sec_name = u_name.encode().decode('unicode_escape')  # unicode转汉字
                round_rate = i['rate']
                date = i['pub_date']
                data_list.append((sec_code, sec_name, round_rate, date))

        except Exception as es:
            logger.error(es)
    end_dt = datetime.datetime.now()
    # 计算采集数据所需时间used_time
    used_time = (end_dt - start_dt).seconds
    data_df = pd.DataFrame(data_list, columns=data_title)
    print(data_df)
    if data_df is not None:
        df_result = {
            'columns': data_title,
            'data': data_df.values.tolist()
        }
        if data_df.iloc[:, 0].size == total:
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_financing_underlying_security, data_source, start_dt,
                                               end_dt, used_time)


# 长城证券融券标的券采集
def rq_target_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集融资数据".format(broker_id))
    url = 'http://www.cgws.com/was5/web/de.jsp'
    page = 1
    page_size = 5
    is_continue = True
    data_list = []
    data_title = ['sec_code', 'sec_name', 'round_rate', 'date']
    start_dt = datetime.datetime.now()
    while is_continue:
        params = {"page": page, "channelid": 257420, "searchword": 'KGNyZWRpdHN0a2N0cmw9MCk=',
                  "_": get_timestamp()}
        try:
            response = requests.get(url=url, params=params, headers=cc_headers, timeout=10)
            text = json.loads(response.text)
            row_list = text['rows']
            total = 0
            if row_list:
                total = int(text['total'])
            print(total)
            if total is not None and type(total) is not str and total > page * page_size:
                is_continue = True
                page = page + 1
            else:
                is_continue = False
            for i in row_list:
                sec_code = i['code']
                if sec_code == "":  # 一页数据,遇到{"code":"","name":"","rate":"","pub_date":"","market":""}表示完结
                    break
                u_name = i['name'].replace('u', '\\u')  # 把u4fe1u7acbu6cf0转成\\u4fe1\\u7acb\\u6cf0
                sec_name = u_name.encode().decode('unicode_escape')  # unicode转汉字
                round_rate = i['rate']
                date = i['pub_date']
                data_list.append((sec_code, sec_name, round_rate, date))

        except Exception as es:
            logger.error(es)

    end_dt = datetime.datetime.now()
    # 计算采集数据所需时间used_time
    used_time = (end_dt - start_dt).seconds
    data_df = pd.DataFrame(data_list, columns=data_title)
    print(data_df)
    if data_df is not None:
        df_result = {
            'columns': data_title,
            'data': data_df.values.tolist()
        }
        if data_df.iloc[:, 0].size == total:
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_lending_underlying_security, data_source, start_dt,
                                               end_dt, used_time)


# 长城证券担保券采集
def guaranty_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集融资数据".format(broker_id))
    url = 'http://www.cgws.com/was5/web/de.jsp'
    page = 1
    page_size = 5
    is_continue = True
    data_list = []
    data_title = ['sec_code', 'sec_name', 'round_rate', 'date']
    start_dt = datetime.datetime.now()
    while is_continue:
        params = {"page": page, "channelid": 229873, "searchword": None,
                  "_": get_timestamp()}
        # logger.info("{}".format(params))
        try:
            response = requests.get(url=url, params=params, headers=cc_headers, timeout=20)
            text = json.loads(response.text)
            row_list = text['rows']
            total = 0
            if row_list:
                total = int(text['total'])
            if total is not None and type(total) is not str and total > page * page_size:
                is_continue = True
                page = page + 1
            else:
                is_continue = False
            for i in row_list:
                sec_code = i['code']
                if sec_code == "":  # 一页数据,遇到{"code":"","name":"","rate":"","pub_date":"","market":""}表示完结
                    break
                u_name = i['name'].replace('u', '\\u')  # 把u4fe1u7acbu6cf0转成\\u4fe1\\u7acb\\u6cf0
                sec_name = u_name.encode().decode('unicode_escape')  # unicode转汉字
                round_rate = i['rate']
                date = i['pub_date']
                data_list.append((sec_code, sec_name, round_rate, date))

        except Exception as es:
            logger.error(es)

    end_dt = datetime.datetime.now()
    # 计算采集数据所需时间used_time
    used_time = (end_dt - start_dt).seconds
    data_df = pd.DataFrame(data_list, columns=data_title)
    print(data_df)
    if data_df is not None:
        df_result = {
            'columns': data_title,
            'data': data_df.values.tolist()
        }
        if data_df.iloc[:, 0].size == total:
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_guaranty_security, data_source, start_dt,
                                               end_dt, used_time)


def get_timestamp():
    return int(time.time() * 1000)


if __name__ == '__main__':
    # rz_target_collect()
    # rq_target_collect()
    # guaranty_collect()
    fire.Fire()

    # python3 cc_securities_collect.py - rz_target_collect
    # python3 cc_securities_collect.py - rq_target_collect
    # python3 cc_securities_collect.py - guaranty_collect
