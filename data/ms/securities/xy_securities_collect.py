#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/30 13:19
# 兴业证券
import time
import os
import sys
from configparser import ConfigParser
from urllib import parse

import requests
from bs4 import BeautifulSoup


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler
from constants import *
from utils.logs_utils import logger

from utils.exceptions_utils import ProxyTimeOutEx
import os
import xlrd2
import datetime

target_excel_name = '兴业证券融资融券标的证券及保证金比例明细表'
guaranty_excel_name = '兴业证券融资融券可充抵保证金证券及折算率明细表'

guaranty_file_path = './' + 'guaranty.xlsx'
target_file_path = './' + 'target.xlsx'

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path_bd = os.path.join(paths, "兴业证券标的券{}.xlsx".format(datetime.date.today()))
save_excel_file_path_bzj = os.path.join(paths, "兴业证券保证金券{}.xlsx".format(datetime.date.today()))


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '兴业证券'
        self.url = "https://www.xyzq.com.cn/xysec/"


    def rzrq_underlying_securities_collect(self):
        query_date = time.strftime('%Y%m%d', time.localtime())
        title_url = "https://www.xyzq.com.cn/xysec/biz/11116"
        try:
            data = {'word': target_excel_name, "cur": 1}
            self.data_list = []
            encode_data = parse.urlencode(data)
            response = self.get_response(title_url, 0, get_headers(), encode_data)
            soup = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')
            label_div_list = soup.select('.newsBox')
            if label_div_list:
                label_div = label_div_list[0]
                my_id = label_div['myid']
                my_path = label_div['mypath']

                label_span_list = soup.select('.newsBox span')
                sc_newest_date = label_span_list[1].text.strip() + label_span_list[0].text.strip().replace('-', "")
                if query_date != sc_newest_date:
                    logger.info(f'{self.data_source}还未更新今日{query_date}数据')
                elif query_date == sc_newest_date:
                    full_download_page_url = self.url + my_path + my_id
                    response = self.get_response(full_download_page_url, 0, get_headers(), encode_data)
                    soup = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')

                    label_aa_list = soup.select("div .fujian a")
                    if label_aa_list:
                        # download_url = label_aa_list[0]['href']
                        download_url = label_aa_list[0]['myurl']  # 兴业改了html,20210817
                        response = requests.get(download_url, timeout=20)
                        with open(target_file_path, 'wb') as file:
                            file.write(response.content)
                        with open(save_excel_file_path_bd, 'wb') as file:
                            file.write(response.content)
                            excel_file = xlrd2.open_workbook(target_file_path)
                            self.target_collect(excel_file)
        except ProxyTimeOutEx as e:
            pass
        finally:
            remove_file(target_file_path)

    def target_collect(self, excel_file):
        sheet_0 = excel_file.sheet_by_index(0)
        total_row = sheet_0.nrows
        for i in range(1, total_row):
            row = sheet_0.row(i)
            if row is None:
                break
            no = str(row[0].value)
            sec_code = str(row[1].value)
            sec_name = str(row[2].value)
            rz_rate = str(row[4].value)
            rq_rate = str(row[5].value)
            self.data_list.append((no, sec_code, sec_name, rz_rate, rq_rate))
            self.collect_num = len(self.data_list)
        self.total_num = total_row - 1

    def guaranty_securities_collect(self):
        query_date = time.strftime('%Y%m%d', time.localtime())
        title_url = "https://www.xyzq.com.cn/xysec/biz/11117"
        try:
            data = {'word': guaranty_excel_name, "cur": 1}
            self.data_list = []
            encode_data = parse.urlencode(data)
            response = self.get_response(title_url, 0, get_headers(), encode_data)
            soup = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')
            label_div_list = soup.select('.newsBox')
            if label_div_list:
                label_div = label_div_list[0]
                my_id = label_div['myid']
                my_path = label_div['mypath']

                label_span_list = soup.select('.newsBox span')
                sc_newest_date = label_span_list[1].text.strip() + label_span_list[0].text.strip().replace('-', "")
                if query_date != sc_newest_date:
                    logger.info(f'{self.data_source}还未更新今日{query_date}数据')
                elif query_date == sc_newest_date:
                    full_download_page_url = self.url + my_path + my_id
                    response = self.get_response(full_download_page_url, 0, get_headers(), encode_data)
                    soup = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')

                    label_aa_list = soup.select("div .fujian a")
                    if label_aa_list:
                        # download_url = label_aa_list[0]['href']
                        download_url = label_aa_list[0]['myurl']  # 兴业改了html,20210817
                        response = requests.get(download_url, timeout=20)
                        with open(guaranty_file_path, 'wb') as file:
                            file.write(response.content)
                        with open(save_excel_file_path_bzj, 'wb') as file:
                            file.write(response.content)
                            excel_file = xlrd2.open_workbook(guaranty_file_path)
                            self.guaranty_collect(excel_file)
        except ProxyTimeOutEx as e:
            pass
        finally:
            remove_file(guaranty_file_path)

    def guaranty_collect(self, excel_file):
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
            self.data_list.append((no, sec_code, sec_name, discount_rate))
            self.collect_num = len(self.data_list)
        self.total_num = len(self.data_list)


def remove_file(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        raise Exception(e)


if __name__ == '__main__':
    collector = CollectHandler()
    collector.collect_data(eval(sys.argv[1]))
