#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/19 13:14
# 国泰君安
import os
import sys
import concurrent.futures

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils import remove_file
from data.ms.basehandler import BaseHandler
import json
import time
from constants import *
from utils.logs_utils import logger
import datetime


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '国泰君安'
        self.url = 'https://www.gtja.com/cos/rest/margin/path/fuzzy.json'

    def rz_underlying_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        params = {"pageNum": 1, "type": 3, "_": remove_file.get_timestamp(), "stamp": search_date}  # type=3表示融资
        self.data_list = []
        response = self.get_response(self.url, 0, get_headers(), params, None, allow_redirects=False)
        text = json.loads(response.text)
        self.total_num = int(text['total'])
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_list = []
            about_total_page = 300
            partition_page = 50
            for i in range(0, about_total_page, partition_page):
                start_page = i + 1
                if i >= about_total_page - partition_page:
                    end_page = None  # 最后一页
                else:
                    end_page = i + partition_page

                logger.info(f'这是第{i}此循环')
                future = executor.submit(self.collect_by_partition_rz, start_page, end_page, search_date)
                future_list.append(future)

            for r in future_list:
                data_temp_list = r.result()
                if data_temp_list:
                    self.data_list.extend(data_temp_list)
                    self.collect_num = len(self.data_list)

    def collect_by_partition_rz(self, start_page, end_page, search_date):
        data_list = []
        if end_page is None:
            end_page = 100000

        is_continue = True
        retry_count = 5
        while is_continue and start_page <= end_page:
            params = {"pageNum": start_page, "type": 3, "_": remove_file.get_timestamp(),
                      "stamp": search_date}  # type=3表示融资
            try:
                response = self.get_response(self.url, 0, get_headers(), params, None, allow_redirects=False)
                text = json.loads(response.text)
                finance_list = text['finance']
            except Exception as e:
                is_continue = False
                if retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if is_continue and len(finance_list) > 0:
                for i in finance_list:
                    data_list.append(i)
                    self.collect_num = len(data_list)

            if int(len(finance_list)) == 10:
                start_page = start_page + 1
            else:
                is_continue = False

        logger.info(f'此线程采集数据共datalist:{len(data_list)}条')
        return data_list

    def rq_underlying_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        params = {"pageNum": 1, "type": 2, "_": remove_file.get_timestamp(), "stamp": search_date}  # type=3表示融资
        self.data_list = []
        response = self.get_response(self.url, 0, get_headers(), params, None, allow_redirects=False)
        text = json.loads(response.text)
        self.total_num = int(text['total'])
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_list = []
            about_total_page = 300
            partition_page = 50
            for i in range(0, about_total_page, partition_page):
                start_page = i + 1
                if i >= about_total_page - partition_page:
                    end_page = None  # 最后一页
                else:
                    end_page = i + partition_page

                logger.info(f'这是第{i}此循环')
                future = executor.submit(self.collect_by_partition_rq, start_page, end_page, search_date)
                future_list.append(future)

            for r in future_list:
                data_temp_list = r.result()
                if data_temp_list:
                    self.data_list.extend(data_temp_list)
                    self.collect_num = len(self.data_list)

    def collect_by_partition_rq(self, start_page, end_page, search_date):
        data_list = []
        if end_page is None:
            end_page = 100000

        is_continue = True
        retry_count = 5
        while is_continue and start_page <= end_page:
            params = {"pageNum": start_page, "type": 2, "_": remove_file.get_timestamp(),
                      "stamp": search_date}  # type=3表示融资
            try:
                response = self.get_response(self.url, 0, get_headers(), params, None, allow_redirects=False)
                text = json.loads(response.text)
                security_list = text['security']
            except Exception as e:
                is_continue = False
                if retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if is_continue and len(security_list) > 0:
                for i in security_list:
                    data_list.append(i)
                    self.collect_num = len(data_list)

            if int(len(security_list)) == 10:
                start_page = start_page + 1
            else:
                is_continue = False

        logger.info(f'此线程采集数据共datalist:{len(data_list)}条')
        return data_list

    def guaranty_securities_collect(self):
        search_date = self.search_date if self.search_date is not None else datetime.date.today()
        search_date = str(search_date).replace('-', '').replace('/', '')
        params = {"pageNum": 1, "type": 1, "_": remove_file.get_timestamp(), "stamp": search_date}  # type=3表示融资
        self.data_list = []
        response = self.get_response(self.url, 0, get_headers(), params, None, allow_redirects=False)
        text = json.loads(response.text)
        self.total_num = int(text['total'])
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_list = []
            about_total_page = 1600
            partition_page = 200
            for i in range(0, about_total_page, partition_page):
                start_page = i + 1
                if i >= about_total_page - partition_page:
                    end_page = None  # 最后一页
                else:
                    end_page = i + partition_page

                logger.info(f'这是第{i}此循环')
                future = executor.submit(self.collect_by_partition_bzj, start_page, end_page, search_date)
                future_list.append(future)

            for r in future_list:
                data_temp_list = r.result()
                if data_temp_list:
                    self.data_list.extend(data_temp_list)
                    self.collect_num = len(self.data_list)

    def collect_by_partition_bzj(self, start_page, end_page, search_date):
        data_list = []
        if end_page is None:
            end_page = 100000

        is_continue = True
        retry_count = 5
        while is_continue and start_page <= end_page:
            params = {"pageNum": start_page, "type": 1, "_": remove_file.get_timestamp(),
                      "stamp": search_date}  # type=3表示融资
            try:
                response = self.get_response(self.url, 0, get_headers(), params, None, allow_redirects=False)
                text = json.loads(response.text)
                offset_list = text['offset']
            except Exception as e:
                is_continue = False
                if retry_count > 0:
                    retry_count = retry_count - 1
                    time.sleep(5)
                    continue

            if is_continue and len(offset_list) > 0:
                for i in offset_list:
                    data_list.append(i)
                    self.collect_num = len(data_list)

            if int(len(offset_list)) == 10:
                start_page = start_page + 1
            else:
                is_continue = False

        logger.info(f'此线程采集数据共datalist:{len(data_list)}条')
        return data_list


if __name__ == '__main__':
    collector = CollectHandler()
    if len(sys.argv) > 2:
        collector.collect_data(eval(sys.argv[1]), sys.argv[2])
    elif len(sys.argv) == 2:
        collector.collect_data(eval(sys.argv[1]))
    elif len(sys.argv) < 2:
        raise Exception(f'business_type为必输参数')