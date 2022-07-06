#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/29 16:47
# 中信建投
import json
import os
import time
import urllib.request
import pandas as pd
import xlrd2
from bs4 import BeautifulSoup
from constants import *
from data.dao import sh_data_deal
from utils.logs_utils import logger
import fire
import datetime

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '3.1'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '3.2'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = 'zxjt_securities'

broker_id = 10006
guaranty_file_path = './' + str(broker_id) + 'guaranty.xls'
target_file_path = './' + str(broker_id) + 'target.xls'
all_file_path = './' + str(broker_id) + 'all.xls'


# 从html拿到标的和担保的excel路径,下载excel再解析之
def all_collect():
    logger.info("broker_id={}开始采集数据".format(broker_id))
    url = "https://www.csc108.com/kyrz/xxggIndex.jspx"
    # 用 urllib.request.urlopen 方式打开一个URL，服务器端会收到对于该页面的访问请求。由于服务器缺失信息，包括浏览器,操作系统,硬件平台等，将视为一种非正常的访问。
    # 在代码中加入UserAgent信息即可。

    try:
        start_dt = datetime.datetime.now()
        req = urllib.request.Request(url=url, headers=get_headers())
        html_utf8 = urllib.request.urlopen(req, timeout=10).read().decode('utf-8')
        soup = BeautifulSoup(html_utf8, 'html.parser')

        label_a_list = soup.select(".kyrz_title02 .more3")
        for tag_a in label_a_list:
            excel_download_url = tag_a['onclick'][13:-2]
            if "标的证券名单及保证金比例" in excel_download_url:
                response = requests.get(excel_download_url)
                with open(target_file_path, 'wb') as file:
                    file.write(response.content)  # 写excel到当前目录
                    excel_file = xlrd2.open_workbook(target_file_path)
                    target_collect(excel_file)

            elif "可充抵保证金证券名单及折算率" in excel_download_url:
                response = requests.get(excel_download_url)
                with open(guaranty_file_path, 'wb') as file:
                    file.write(response.content)  # 写excel到当前目录
                    excel_file = xlrd2.open_workbook(guaranty_file_path)
                    guaranty_collect(excel_file)
            elif "可充抵保证金证券及标的证券" in excel_download_url:  # 20220323 中信建投网站3种券合成一个excel文件了
                response = requests.get(excel_download_url)
                with open(all_file_path, 'wb') as file:
                    file.write(response.content)  # 写excel到当前目录
                    excel_file = xlrd2.open_workbook(all_file_path)
                    do_all_collect(excel_file, all_file_path)
    except Exception as es:
        print(es)
    finally:
        remove_file(all_file_path)


def target_collect(excel_file):
    pass


def guaranty_collect(excel_file):
    pass


def do_all_collect(excel_file, all_file_path):
    """
    解析excel文件且分别入库
    :param excel_file:
    :return:
    """
    query_date = time.strftime('%Y%m%d', time.localtime())
    try:
        start_dt = datetime.datetime.now()
        sheet_0 = excel_file.sheet_by_index(0)
        total_row = sheet_0.nrows
        data_list = []

        for i in range(2, total_row):  # 从第3行开始遍历
            row = sheet_0.row(i)
            if row is None:
                break
            no = str(row[0].value)
            sec_code = str(row[1].value)
            market = str(row[2].value)
            sec_name = str(row[3].value)
            db_rate_str = str(row[4].value)
            rz_rate_str = str(row[5].value)
            rq_rate_str = str(row[6].value)
            rz_valid_status = str(row[7].value)
            rq_valid_status = str(row[8].value)
            # rz_valid_status = '0' if str(row[7].value).strip() == '允许' else '1'
            # rq_valid_status = '0' if str(row[8].value).strip() == '允许' else '1'
            data_list.append(
                (no, sec_code, market, sec_name, db_rate_str, rz_rate_str, rq_rate_str, rz_valid_status,
                 rq_valid_status))

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        data_df = pd.DataFrame(data_list,
                               columns=['no', 'sec_code', 'market', 'sec_name', 'db_rate_str', 'rz_rate_str',
                                        'rq_rate_str',
                                        'rz_valid_status', 'rq_valid_status'])
        print(data_df)
        if data_df is not None:
            df_result = {
                'columns': ['no', 'sec_code', 'market', 'sec_name', 'db_rate_str', 'rz_rate_str', 'rq_rate_str',
                            'rz_valid_status', 'rq_valid_status'],
                'data': data_df.values.tolist()
            }
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                             , exchange_mt_guaranty_and_underlying_security, data_source, start_dt,
                                               end_dt, used_time, all_file_path)
    except Exception as es:
        logger.error(es)


def remove_file(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        logger.info("删除文件异常:{}".format(e))


if __name__ == '__main__':
    # all_collect()
    fire.Fire()

    # python3 zxjt_securities_collect.py - all_collect
