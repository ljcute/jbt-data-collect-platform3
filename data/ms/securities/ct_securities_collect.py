#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 13:19
# 财通证券
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

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '3.1'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '3.2'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = 'ct_securities'


# 财通证券融资融券标的证券采集
def target_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集财通证券融资融券标的证券数据".format(broker_id))
    url = 'https://www.ctsec.com/business/equityList'
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Length': '63',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': 'Hm_lvt_2f65d6872cec3a2a39813c2c9c9126bb=1656386163; '
                  'Hm_lpvt_2f65d6872cec3a2a39813c2c9c9126bb=1656577504',
        'Host': 'www.ctsec.com',
        'Origin': 'https://www.ctsec.com',
        'Referer': 'https://www.ctsec.com/business/financing/equity',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': random.choice(USER_AGENTS),
        'X-Requested-With': 'XMLHttpRequest'
    }
    data = {
        'init_date': "2022/06/30",
        'page': 1,
        'size': 10000
    }
    try:
        start_dt = datetime.datetime.now()
        response = requests.post(url=url, data=data, headers=headers)
        data_list = []
        data_title = ['sec_code', 'sec_name', 'rz_rate', 'rq_rate']
        if response.status_code == 200:
            text = json.loads(response.text)
            total = text['data']['total']
            data = text['data']['rows']
            print(data)
            if data:
                for i in data:
                    sec_code = i['STOCK_CODE']
                    sec_name = i['STOCK_NAME']
                    rz_rate = i['FIN_RATIO']  # 融资保证金比例
                    rq_rate = i['SLO_RATIO']  # 融券保证金比例
                    data_list.append((sec_code, sec_name, rz_rate, rq_rate))

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
                                                           , exchange_mt_underlying_security, data_source, start_dt,
                                                           end_dt, used_time)

    except Exception as es:
        logger.error(es)


# 财通证券可充抵保证金证券采集
def guaranty_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集财通证券可充抵保证金证券数据".format(broker_id))
    url = 'https://www.ctsec.com/business/getAssureList'
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Length': '71',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': 'Hm_lvt_2f65d6872cec3a2a39813c2c9c9126bb=1656386163; '
                  'Hm_lpvt_2f65d6872cec3a2a39813c2c9c9126bb=1656577524',
        'Host': 'www.ctsec.com',
        'Origin': 'https://www.ctsec.com',
        'Referer': 'https://www.ctsec.com/business/financing/butFor',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': random.choice(USER_AGENTS),
        'X-Requested-With': 'XMLHttpRequest'
    }
    data = {
        'init_date': "2022/06/30",
        'page': 1,
        'size': 10000
    }
    try:
        start_dt = datetime.datetime.now()
        response = requests.post(url=url, data=data, headers=headers)
        data_list = []
        data_title = ['sec_code', 'sec_name', 'discount_rate', 'market']
        if response.status_code == 200:
            text = json.loads(response.text)
            total = text['data']['total']
            data = text['data']['rows']
            print(data)
            if data:
                for i in data:
                    sec_code = i['STOCK_CODE']
                    sec_name = i['STOCK_NAME']
                    discount_rate = i['ASSURE_RATIO']  # 融资保证金比例
                    market = i['MARKET']  # 融券保证金比例
                    data_list.append((sec_code, sec_name, discount_rate, market))

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

    except Exception as es:
        logger.error(es)


if __name__ == '__main__':
    # target_collect()
    # guaranty_collect()
    fire.Fire()
    # python3 ct_securities_collect.py - target_collect
    # python3 ct_securities_collect.py - guaranty_collect
