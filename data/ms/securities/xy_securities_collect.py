#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 13:19
# 兴业证券
import os
import json
import time
import xlrd2
import pandas as pd
from constants import *
from data.dao import sh_data_deal
from utils.logs_utils import logger
import datetime
import fire

# 定义常量
broker_id = 10011

target_excel_name = '兴业证券融资融券标的证券及保证金比例明细表'
guaranty_excel_name = '兴业证券融资融券可充抵保证金证券及折算率明细表'

guaranty_file_path = './' + str(broker_id) + 'guaranty.xlsx'
target_file_path = './' + str(broker_id) + 'target.xlsx'

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '3.1'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '3.2'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = 'xy_securities'


# 兴业证券标的相关数据采集
def target_collect_task():
    logger.info("broker_id={}开始采集兴业证券融资融券融资标的相关数据".format(broker_id))
    excel_one_download_url = "https://static.xyzq.cn/xywebsite/attachment/3B8333A8CD0845A9A2.xlsx"

    try:
        response = requests.get(excel_one_download_url)
        if response.status_code == 200:
            with open(target_file_path, 'wb') as file:
                file.write(response.content)
                excel_file = xlrd2.open_workbook(target_file_path)
                target_collect(excel_file, target_file_path)

    except Exception as es:
        logger.error(es)
    finally:
        remove_file(target_file_path)


def target_collect(excel_file, target_file_path):
    query_date = time.strftime('%Y%m%d', time.localtime())
    try:
        start_dt = datetime.datetime.now()
        sheet_0 = excel_file.sheet_by_index(0)
        total_row = sheet_0.nrows
        data_list = []
        data_tile = ['no', 'sec_code', 'sec_name', 'rz_rate', 'rq_rate']
        for i in range(1, total_row):
            row = sheet_0.row(i)
            if row is None:
                break
            no = str(row[0].value)
            sec_code = str(row[1].value)
            sec_name = str(row[2].value)
            rz_rate = str(row[4].value)
            rq_rate = str(row[5].value)

            data_list.append((no, sec_code, sec_name, rz_rate, rq_rate))

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        data_df = pd.DataFrame(data_list, columns=data_tile)
        print(data_df)
        if data_df is not None:
            df_result = {
                'columns': data_tile,
                'data': data_df.values.tolist()
            }
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_underlying_security, data_source, start_dt,
                                               end_dt, used_time, target_file_path, )

    except Exception as es:
        logger.error(es)


# 兴业证券融资融券可充抵保证金证券及折算率数据采集
def guaranty_collect_task():
    logger.info("broker_id={}开始采集兴业证券融资融券可充抵保证金证券及折算率相关数据".format(broker_id))
    excel_two_download_url = "https://static.xyzq.cn/xywebsite/attachment/B21E17122E41411497.xlsx"

    try:
        response = requests.get(excel_two_download_url)
        if response.status_code == 200:
            with open(guaranty_file_path, 'wb') as file:
                file.write(response.content)
                excel_file = xlrd2.open_workbook(guaranty_file_path)
                guaranty_collect(excel_file, guaranty_file_path)

    except Exception as es:
        print(es)
    finally:
        remove_file(guaranty_file_path)


def guaranty_collect(excel_file, guaranty_file_path):
    query_date = time.strftime('%Y%m%d', time.localtime())
    try:
        start_dt = datetime.datetime.now()
        sheet_0 = excel_file.sheet_by_index(0)
        total_row = sheet_0.nrows
        data_list = []
        data_tile = ['no', 'sec_code', 'sec_name', 'discount_rate']
        current_read_part = 1  # 担保券excel文件的sheet0分两部份，要遍历2次
        total_part = 2
        reading_row_num = 1  # 从第2行开始遍历
        reading_column_num = 0  # 从第几列开始读,第一部份是第0列,第二部分是第5列,用于判断该部份是否有数据，没有数据就认为结束
        while reading_row_num < total_row and current_read_part <= total_part:
            row = sheet_0.row(reading_row_num)
            if row[reading_column_num].value == '':
                reading_row_num = 1  # 读到空行结束当前部份，重置行号
                current_read_part = current_read_part + 1  # 下一部份
                reading_column_num = reading_column_num + 5
                continue

            reading_row_num = reading_row_num + 1  # 行号+1

            if current_read_part == 1:  # 读第1部份数据(左侧)
                no = str(row[0].value)
                sec_code = str(row[1].value)
                sec_name = str(row[2].value)
                discount_rate = str(row[3].value)
            else:  # 读第2部份数据(右侧)
                no = str(row[5].value)
                sec_code = str(row[6].value)
                sec_name = str(row[7].value)
                discount_rate = str(row[8].value)

            data_list.append((no, sec_code, sec_name, discount_rate))

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        data_df = pd.DataFrame(data_list, columns=data_tile)
        print(data_df)
        if data_df is not None:
            df_result = {
                'columns': data_tile,
                'data': data_df.values.tolist()
            }
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_guaranty_security, data_source, start_dt,
                                               end_dt, used_time, guaranty_file_path)

    except Exception as es:
        logger.error(es)


def remove_file(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        logger.error("删除文件异常:{}".format(e))


if __name__ == '__main__':
    # target_collect_task()
    # guaranty_collect_task()

    fire.Fire()

    # python3 xy_securities_collect.py - target_collect_task
    # python3 xy_securities_collect.py - guaranty_collect_task
