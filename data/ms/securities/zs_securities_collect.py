#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 10:19
# 招商证券 --interface

import json
import time
import pandas as pd
from constants import *
from data.dao import sh_data_deal
from utils.logs_utils import logger
import datetime
import fire

# 定义常量
broker_id = 10005

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '3.1'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '3.2'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = 'zs_securities'


# 标的证券及保证金比例采集
def rz_target_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集招商证券标的证券及保证金比例数据".format(broker_id))
    url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstock'
    page_size = random_page_size()
    params = {"pageSize": page_size, "pageNum": 1, "rqbdflag": 1}  # rqbdflag = 1融资
    logger.info("{}".format(params))
    try:
        start_dt = datetime.datetime.now()
        response = requests.get(url=url, params=params, headers=get_headers(), timeout=5)
        if response.status_code == 200:
            text = json.loads(response.text)
            total = text['body']['totalNum']
            data_list = text['body']['stocks']
            print(total)
            target_title = ['stock_code', 'stock_name', 'margin_rate']
            target_list = []
            for i in data_list:
                stock_code = i['stkcode']
                stock_name = i['stkname']
                margin_rate = i['marginratefund']
                target_list.append((stock_code, stock_name, margin_rate))

            end_dt = datetime.datetime.now()
            # 计算采集数据所需时间used_time
            used_time = (end_dt - start_dt).seconds
            data_df = pd.DataFrame(target_list, columns=target_title)
            print(data_df)
            if data_df is not None:
                df_result = {
                    'columns': target_title,
                    'data': data_df.values.tolist()
                }
                sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                                   , exchange_mt_underlying_security, data_source, start_dt,
                                                   end_dt, used_time)
    except Exception as es:
        logger.error(es)


# 可充抵保证金证券及折算率采集
def guaranty_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集可充抵保证金证券及折算率数据".format(broker_id))
    url = 'https://www.cmschina.com/api/newone2019/rzrq/rzrqstockdiscount'
    page_size = random_page_size()
    params = {"pageSize": page_size, "pageNum": 1}
    logger.info("{}".format(params))
    try:
        start_dt = datetime.datetime.now()
        response = requests.get(url=url, params=params, headers=get_headers(), timeout=5)
        if response.status_code == 200:
            text = json.loads(response.text)
            total = text['body']['totalNum']
            data_list = text['body']['stocks']
            print(total)
            target_title = ['stock_code', 'stock_name', 'discount_rate']
            target_list = []
            for i in data_list:
                stock_code = i['stkcode']
                stock_name = i['stkname']
                margin_rate = i['pledgerate']
                target_list.append((stock_code, stock_name, margin_rate))

            end_dt = datetime.datetime.now()
            # 计算采集数据所需时间used_time
            used_time = (end_dt - start_dt).seconds
            data_df = pd.DataFrame(target_list, columns=target_title)
            print(data_df)
            if data_df is not None:
                df_result = {
                    'columns': target_title,
                    'data': data_df.values.tolist()
                }
                sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                                   , exchange_mt_underlying_security, data_source, start_dt,
                                                   end_dt, used_time)
                logger.info("broker_id={}完成数据入库".format(broker_id))
    except Exception as es:
        logger.error(es)


def random_page_size(mu=28888, sigma=78888):
    """
    获取随机分页数
    :param mu:
    :param sigma:
    :return:
    """
    random_value = random.randint(mu, sigma)  # Return random integer in range [a, b], including both end points.
    return random_value


if __name__ == '__main__':
    # rz_target_collect()
    # guaranty_collect()
    fire.Fire()

    # python3 zs_securities_collect.py - rz_target_collect
    # python3 zs_securities_collect.py - guaranty_collect
