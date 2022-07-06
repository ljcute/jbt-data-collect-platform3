#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/27 09:33
import pandas as pd
import json
import time
import traceback
import xlrd2
from utils.proxy_utils import get_proxies
import requests
import os
import fire
import datetime
from data.dao import sh_data_deal
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sz_exchange_mt_underlying_and_guaranty_security.xlsx')

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '3.1'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '3.2'  # 融资融券融券标的证券

broker_id = 10000

sh_guaranty_file_path = './' + str(broker_id) + 'sh_guaranty.xls'
sh_target_rz_file_path = './' + str(broker_id) + 'sh_target_rz.xls'
sh_target_rq_file_path = './' + str(broker_id) + 'sh_target_rq.xls'

data_source_szse = 'szse'
data_source_sse = 'sse'


def get_data():
    url_guaranty = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?FLAG=003&sqlId=COMMON_SSE_FW_JYFW_RZRQ_JYXX_BDZQKCDBZJZQLB_RZRQKCDBZJ_L"
    url_rz = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?FLAG=001&sqlId=COMMON_SSE_FW_JYFW_RZRQ_JYXX_BDZQKCDBZJZQLB_RZMRBDZQ_L"
    url_rq = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?FLAG=002&sqlId=COMMON_SSE_FW_JYFW_RZRQ_JYXX_BDZQKCDBZJZQLB_RZMCBDZQ_L"
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cookie': 'yfx_c_g_u_id_10000042=_ck22060711191911485514723010297; '
                  'VISITED_MENU=%5B%228307%22%2C%229729%22%5D; JSESSIONID=771FCD96DF812328467D7B327B093D35; '
                  'gdp_user_id=gioenc-6e004388%2C3d26%2C59c4%2C838g%2C4063ea3a9528; '
                  'ba17301551dcbaf9_gdp_session_id=4a6d84c6-2cd3-4b35-b7eb-4286992ff745; '
                  'ba17301551dcbaf9_gdp_session_id_4a6d84c6-2cd3-4b35-b7eb-4286992ff745=true; '
                  'yfx_f_l_v_t_10000042=f_t_1654571959111__r_t_1655691628385__v_t_1655692038721__r_c_5',
        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36'
    }
    local_date = time.strftime('%Y-%m-%d', time.localtime())
    try:
        logger.info("上交所、担保券开始采集")
        response = requests.get(url=url_guaranty, proxies=get_proxies(), headers=headers, timeout=5)
        with open(sh_guaranty_file_path, 'wb') as file:
            file.write(response.content)  # 写excel到当前目录
            excel_file = xlrd2.open_workbook(sh_guaranty_file_path)
            handle_excel(excel_file, local_date, sh_guaranty_file_path, exchange_mt_guaranty_security, data_source_sse)

        logger.info("上交所、融资开始采集")
        response = requests.get(url=url_rz, headers=headers, timeout=5)
        with open(sh_target_rz_file_path, 'wb') as file:
            file.write(response.content)  # 写excel到当前目录
            excel_file = xlrd2.open_workbook(sh_target_rz_file_path)
            handle_excel(excel_file, local_date, sh_target_rz_file_path, exchange_mt_financing_underlying_security,
                         data_source_sse)

        logger.info("上交所、融券开始采集")
        response = requests.get(url=url_rq, headers=headers, timeout=5)
        with open(sh_target_rq_file_path, 'wb') as file:
            file.write(response.content)  # 写excel到当前目录
            excel_file = xlrd2.open_workbook(sh_target_rq_file_path)
            handle_excel(excel_file, local_date, sh_target_rq_file_path, exchange_mt_lending_underlying_security,
                         data_source_sse)

    except Exception as es:
        logger.error("采集上交所全量两融券异常,{}".format(es))
        traceback.format_tb()
        print(es)
    finally:
        remove_file(sh_guaranty_file_path)
        remove_file(sh_target_rz_file_path)
        remove_file(sh_target_rq_file_path)


def handle_excel(excel_file, date, excel_file_path, type, soure):
    start_dt = datetime.datetime.now()
    sheet_0 = excel_file.sheet_by_index(0)
    total_row = sheet_0.nrows
    try:
        data_list = []
        for i in range(1, total_row):  # 从第2行开始遍历
            row = sheet_0.row(i)
            if row is None:
                break
            biz_dt = row[0].value
            sec_code = str(row[1].value)
            sec_name = str(row[2].value)
            data_list.append((biz_dt, sec_code, sec_name))

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        data_df = pd.DataFrame(data_list, columns=['biz_dt', 'sec_code', 'sec_name'])
        print(data_df)
        if data_df is not None:
            if data_df.iloc[:, 0].size == total_row - 1:
                df_result = {
                    'columns': ['biz_dt', 'sec_code', 'sec_name'],
                    'data': data_df.values.tolist()
                }
                sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), date
                                                   , type, soure, start_dt,
                                                   end_dt, used_time, excel_file_path)

    except Exception as es:
        logger.error(es)


def remove_file(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        logger.info("删除文件异常:{}".format(e))


if __name__ == '__main__':
    # get_data()
    fire.Fire()

    # python3 sh_exchange_mt_underlying_and_guaranty_security.py - get_data
