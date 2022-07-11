#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 13:19
# 长江证券
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
import json
import time
import pandas as pd
from constants import *
from data.dao import data_deal
from utils.logs_utils import logger
import datetime

# 定义常量
broker_id = 10005
exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '长江证券'


# 长江证券标的证券及保证金比例采集
def target_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集长江证券标的证券及保证金比例数据".format(broker_id))
    url = 'https://www.95579.com/servlet/json'
    params = {"funcNo": "902122", "i_page": 1, "i_perpage": 10000}  # 默认查询当天
    try:
        response = requests.get(url=url, params=params, headers=get_headers(), timeout=10)
        if response.status_code == 200:
            start_dt = datetime.datetime.now()
            text = json.loads(response.text)
            data_list = text['results']
            target_list = []
            if len(data_list) > 0:
                target_title = ['market', 'stock_code', 'stock_name', 'rzbd', 'rqbz']
                total = int(data_list[0]['total_rows'])
                for i in data_list:
                    stock_code = i['stock_code']
                    stock_name = i['stock_name']
                    rz_rate = i['fin_ratio']
                    rq_rate = i['bail_ratio']
                    # market = '深圳' if i['exchange_type'] == '2' else '上海'
                    market = i['exchange_type']
                    rzbd = i['rzbd']
                    rqbd = i['rqbd']
                    target_list.append((market, stock_code, stock_name, rzbd, rqbd))

                logger.info("broker_id={}采集长江证券标的证券及保证金比例数据结束".format(broker_id))
                end_dt = datetime.datetime.now()
                # 计算采集数据所需时间used_time
                used_time = (end_dt - start_dt).seconds
                data_df = pd.DataFrame(target_list, columns=target_title)
                if data_df is not None:
                    df_result = {
                        'columns': target_title,
                        'data': data_df.values.tolist()
                    }
                    if data_df.iloc[:, 0].size == total:
                        data_deal.insert_data_collect(json.dumps(df_result, ensure_ascii=False), query_date
                                                      , exchange_mt_underlying_security, data_source, start_dt,
                                                      end_dt, used_time, url)
                        logger.info("broker_id={}数据采集完成，已成功入库！".format(broker_id))

            else:
                logger.info("无长江证券标的证券及保证金比例数据")

    except Exception as es:
        logger.error(es)


# 长江证券可充抵保证金证券及折算率采集
def guaranty_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集长江证券可充抵保证金证券及折算率数据".format(broker_id))
    url = 'https://www.95579.com/servlet/json'
    params = {"funcNo": "902124", "i_page": 1, "i_perpage": 10000}  # 默认查询当天
    try:
        response = requests.post(url=url, params=params, headers=get_headers(), timeout=10)
        if response.status_code == 200:
            start_dt = datetime.datetime.now()
            text = json.loads(response.text)
            data_list = text['results']
            target_list = []
            if len(data_list) > 0:
                target_title = ['market', 'stock_code', 'stock_name', 'discount_rate']
                total = int(data_list[0]['total_rows'])
                for i in data_list:
                    stock_code = i['stock_code']
                    stock_name = i['stock_name']
                    discount_rate = i['assure_ratio']
                    market = i['exchange_type']
                    target_list.append((market, stock_code, stock_name, discount_rate))

                logger.info("broker_id={}采集长江证券可充抵保证金证券及折算率数据采集结束".format(broker_id))
                end_dt = datetime.datetime.now()
                # 计算采集数据所需时间used_time
                used_time = (end_dt - start_dt).seconds
                data_df = pd.DataFrame(target_list, columns=target_title)
                if data_df is not None:
                    df_result = {
                        'columns': target_title,
                        'data': data_df.values.tolist()
                    }
                    if data_df.iloc[:, 0].size == total:
                        data_deal.insert_data_collect(json.dumps(df_result, ensure_ascii=False), query_date
                                                      , exchange_mt_guaranty_security, data_source, start_dt,
                                                      end_dt, used_time, url)
                        logger.info("broker_id={}数据采集完成，已成功入库！".format(broker_id))

            else:
                logger.info("无长江证券可充抵保证金证券及折算率数据")

    except Exception as es:
        logger.error(es)


if __name__ == '__main__':
    target_collect()
    guaranty_collect()

    # fire.Fire()

    # python3 cj_securities_collect.py - target_collect
    # python3 cj_securities_collect.py - guaranty_collect
