#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/27 09:33
# 上海交易所-融资融券可充抵保证金证券及融资/融券标的证券

import os
import sys
from configparser import ConfigParser

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler, get_proxies

import xlrd2
import os
import datetime
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sz_exchange_mt_underlying_and_guaranty_security.xlsx')

sh_guaranty_file_path = './' + 'sh_guaranty.xls'
sh_target_rz_file_path = './' + 'sh_target_rz.xls'
sh_target_rq_file_path = './' + 'sh_target_rq.xls'

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path_gu = os.path.join(paths, "上交所担保券{}.xls".format(datetime.date.today()))
save_excel_file_path_rz = os.path.join(paths, "上交所融资标的券{}.xls".format(datetime.date.today()))
save_excel_file_path_rq = os.path.join(paths, "上交所融券标的券{}.xls".format(datetime.date.today()))


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '上海交易所'
        self.headers = {
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

    def rz_underlying_securities_collect(self):
        query_date = self.search_date if self.search_date is not None else datetime.date.today()
        actual_date = datetime.date.today() if query_date is None else query_date
        self.url = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?FLAG=001&sqlId=COMMON_SSE_FW_JYFW_RZRQ_JYXX_BDZQKCDBZJZQLB_RZMRBDZQ_L"
        response = self.get_response(self.url, 0, self.headers)
        download_excel(response, sh_target_rz_file_path, save_excel_file_path_rz, actual_date)
        excel_file = xlrd2.open_workbook(sh_target_rz_file_path, encoding_override="utf-8")
        self.biz_dt, self.data_list, total_num = handle_excel(excel_file, actual_date)
        self.collect_num = self.total_num = len(self.data_list)

    def rq_underlying_securities_collect(self):
        query_date = self.search_date if self.search_date is not None else datetime.date.today()
        actual_date = datetime.date.today() if query_date is None else query_date
        self.url = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?FLAG=002&sqlId=COMMON_SSE_FW_JYFW_RZRQ_JYXX_BDZQKCDBZJZQLB_RZMCBDZQ_L"
        response = self.get_response(self.url, 0, self.headers)
        download_excel(response, sh_target_rq_file_path, save_excel_file_path_rq, actual_date)
        excel_file = xlrd2.open_workbook(sh_target_rq_file_path, encoding_override="utf-8")
        self.biz_dt, self.data_list, total_num = handle_excel(excel_file, actual_date)
        self.collect_num = self.total_num =  len(self.data_list)

    def guaranty_securities_collect(self):
        query_date = self.search_date if self.search_date is not None else datetime.date.today()
        actual_date = datetime.date.today() if query_date is None else query_date
        self.url = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?FLAG=003&sqlId=COMMON_SSE_FW_JYFW_RZRQ_JYXX_BDZQKCDBZJZQLB_RZRQKCDBZJ_L"
        response = self.get_response(self.url, 0, self.headers)
        download_excel(response, sh_guaranty_file_path, save_excel_file_path_gu, actual_date)
        excel_file = xlrd2.open_workbook(sh_guaranty_file_path, encoding_override="utf-8")
        self.biz_dt, self.data_list, total_num = handle_excel(excel_file, actual_date)
        self.collect_num = len(self.data_list)
        self.collect_num = self.total_num =  len(self.data_list)


def download_excel(response, excel_file_path, save_excel_file_path, query_date=None):
    try:
        with open(excel_file_path, 'wb') as file:
            file.write(response.content)
        with open(save_excel_file_path, 'wb') as file:
            file.write(response.content)
    except Exception as es:
        raise Exception(es)


def handle_excel(excel_file, date):
    logger.info("开始处理excel")
    sheet_0 = excel_file.sheet_by_index(0)
    total_row = sheet_0.nrows
    biz_dt = None
    try:
        logger.info("开始处理excel数据")
        data_list = []
        for i in range(1, total_row):  # 从第2行开始遍历
            row = sheet_0.row(i)
            if row is None:
                break
            biz_dt = row[0].value
            sec_code = str(row[1].value)
            sec_name = str(row[2].value)
            data_list.append((biz_dt, sec_code, sec_name))

        logger.info("采集上交所数据结束")
        return biz_dt, data_list, total_row

    except Exception as es:
        raise Exception(es)


if __name__ == '__main__':
    CollectHandler().collect_data(4)
    CollectHandler().collect_data(5)
    #
    # collector = CollectHandler()
    # if len(sys.argv) > 1:
    #     collector.collect_data(eval(sys.argv[1]))
    # else:
    #     logger.error(f'business_type为必传参数')
