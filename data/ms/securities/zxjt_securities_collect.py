#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/29 16:47
# 中信建投

import os
import sys
from configparser import ConfigParser

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder

import json
import os
import time
import urllib.request
import xlrd2
from bs4 import BeautifulSoup
from constants import *
from utils.logs_utils import logger
import datetime

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '中信建投'

broker_id = 10006
guaranty_file_path = './' + str(broker_id) + 'guaranty.xls'
target_file_path = './' + str(broker_id) + 'target.xls'
all_file_path = './' + str(broker_id) + 'all.xls'

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, '中信建投证券三种数据整合.xls')


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type):
        max_retry = 0
        while max_retry < 3:
            try:
                if business_type:
                    if business_type == 99:
                        cls.all_collect()
                    else:
                        logger.error(f'business_type{business_type}输入有误，请检查！')

                break

            except Exception as e:
                time.sleep(3)
                logger.error(e)

            max_retry += 1

    # 从html拿到标的和担保的excel路径,下载excel再解析之
    @classmethod
    def all_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集中信建投数据{actual_date}')
        url = "https://www.csc108.com/kyrz/xxggIndex.jspx"
        # 用 urllib.request.urlopen 方式打开一个URL，服务器端会收到对于该页面的访问请求。由于服务器缺失信息，包括浏览器,操作系统,硬件平台等，将视为一种非正常的访问。
        # 在代码中加入UserAgent信息即可。

        try:
            proxies = super().get_proxies()
            proxy = None
            if proxies is not None:
                proxy_1 = proxies['http']
            else:
                logger.error(f'代理{proxies}获取失败，停止采集！')
                raise Exception(f'代理{proxies}获取失败，停止采集！')
            proxy_1 = proxy_1[7:]
            proxy_dict = {}
            proxy_dict['http'] = proxy_1
            handle = urllib.request.ProxyHandler(proxies=proxy_dict)
            opener = urllib.request.build_opener(handle)
            urllib.request.install_opener(opener)
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
                        logger.info("开始处理下载excel")
                        file.write(response.content)  # 写excel到当前目录
                    with open(save_excel_file_path, 'wb') as file:
                        file.write(response.content)
                        excel_file = xlrd2.open_workbook(all_file_path)
                        cls.do_all_collect(excel_file, all_file_path, url)
                        logger.info("处理excel完成")

        except Exception as es:
            logger.error(es)
        finally:
            remove_file(all_file_path)

    @classmethod
    def do_all_collect(cls, excel_file, all_file_path, url):
        """
        解析excel文件且分别入库
        :param excel_file:
        :return:
        """
        actual_date = datetime.date.today()
        try:
            start_dt = datetime.datetime.now()
            sheet_0 = excel_file.sheet_by_index(0)
            total_row = sheet_0.nrows
            data_list = []
            title_list = ['no', 'sec_code', 'market', 'sec_name', 'db_rate_str', 'rz_rate_str', 'rq_rate_str',
                          'rz_valid_status', 'rq_valid_status']
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

            logger.info(f'采集中信建投数据结束共{int(len(data_list))}条')
            df_result = super().data_deal(data_list, title_list)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds

            if df_result is not None:
                super().data_insert(int(len(data_list)), df_result, actual_date,
                                    exchange_mt_guaranty_and_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url,
                                    save_excel_file_path)
                logger.info(f'入库信息,共{int(len(data_list))}条')
            else:
                raise Exception("采集数据条数为0，入库失败")

            message = "中信建投数据采集完成"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_guaranty_and_underlying_security, data_source, message)

            logger.info("中信建投数据采集完成")

        except Exception as es:
            logger.error(es)


def target_collect(excel_file):
    pass


def guaranty_collect(excel_file):
    pass


def remove_file(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        logger.info("删除文件异常:{}".format(e))


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(99)
    collector.collect_data(eval(sys.argv[1]))
