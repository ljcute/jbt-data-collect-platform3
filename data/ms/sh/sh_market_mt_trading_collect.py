#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/12 17:05
# @Site    :
# @Software: PyCharm
# 上海交易所-融资融券交易汇总及详细数据
import sys
import time
import xlrd2
import datetime
import os
from configparser import ConfigParser
from selenium.webdriver.common.by import By
from constants import get_headers

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from utils.logs_utils import logger
from data.ms.basehandler import BaseHandler

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sh_balance.xls')

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, "上交所融资融券{}.xls".format(datetime.date.today()))



class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '上海交易所'

    def trading_amount_collect(self):
        trade_date = self.get_trade_date()
        self.biz_dt = trade_date
        download_excel_url = "http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk20220623.xls"
        replace_str = 'rzrqjygk' + str(trade_date).format("'%Y%m%d'").replace('-', '') + '.xls'
        download_excel_url = download_excel_url.replace(download_excel_url.split('/')[-1], replace_str)
        self.url = download_excel_url
        response = self.get_response(self.url, 0, get_headers())
        download_excel(response)
        excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
        self.data_list, total_row = handle_excel_total(excel_file, trade_date)
        self.total_num = total_row - 17
        self.collect_num = len(self.data_list)

    def trading_items_collect(self):
        trade_date = self.get_trade_date()
        download_excel_url = "http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk20220623.xls"
        replace_str = 'rzrqjygk' + str(trade_date).format("'%Y%m%d'").replace('-', '') + '.xls'
        download_excel_url = download_excel_url.replace(download_excel_url.split('/')[-1], replace_str)
        self.url = download_excel_url
        response = self.get_response(self.url, 0)
        download_excel(response)
        excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
        self.data_list, total_row = handle_excel_detail(excel_file, trade_date)
        self.total_num = total_row - 1
        self.collect_num = len(self.data_list)

    def get_trade_date(self):
        try:
            logger.info(f'开始获取上海交易所最新交易日日期')
            driver = self.get_driver()
            url = 'http://www.sse.com.cn/market/othersdata/margin/detail/'
            driver.get(url)
            time.sleep(3)
            trade_date = driver.find_elements(By.XPATH, '/html/body/div[8]/div/div[2]/div/div[1]/div[1]/div[1]/table/tbody/tr/td[1]')[0].text
            logger.info(f'上海交易所最新交易日日期为{trade_date}')
            return trade_date
        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            raise Exception(e)


def download_excel(response, query_date=None):
    if response.status_code == 200:
        try:
            with open(excel_file_path, 'wb') as file:
                file.write(response.content)  # 写excel到当前目录
            with open(save_excel_file_path, 'wb') as file:
                file.write(response.content)  # 写excel到当前目录
        except Exception as es:
            raise Exception(es)
    else:
        logger.info("上交所该日无数据:txt_date:{}".format(query_date))


# 汇总信息
def handle_excel_total(excel_file, date):
    logger.info("开始处理excel")
    url = 'http://www.sse.com.cn/market/othersdata/margin/sum/'
    start_dt = datetime.datetime.now()
    sheet_0 = excel_file.sheet_by_index(0)
    total_row = sheet_0.nrows
    try:
        data_list = []
        for i in range(1, 2):  # 从第2行开始遍历
            row = sheet_0.row(i)
            if row is None:
                break
            date = str(date).format("'%Y%m%d'").replace('-', '')  # 信用交易日期
            rzye = float(str(row[0].value).replace(",", ""))  # 融资余额（元）
            rzmre = float(str(row[1].value).replace(",", ""))  # 融资买入额（元）
            rjyl = str(row[2].value).replace(",", "")  # 融券余量
            rjylje = float(str(row[3].value).replace(",", ""))  # 融券余量金额(元)
            rjmcl = str(row[4].value).replace(",", "")  # 融券卖出量
            rzrjye = float(str(row[5].value).replace(",", ""))  # 融资融券余额(元)
            data_list.append((date, rzye, rzmre, rjyl, rjylje, rjmcl, rzrjye))

        logger.info("excel处理完成，开始处理汇总数据")
        return data_list, total_row
    except Exception as e:
        raise Exception(e)


# 详细信息
def handle_excel_detail(excel_file, date):
    logger.info("开始解析excel，处理数据！")
    url = 'http://www.sse.com.cn/market/othersdata/margin/sum/'
    start_dt = datetime.datetime.now()
    sheet_1 = excel_file.sheet_by_index(1)
    total_row = sheet_1.nrows
    try:
        data_list = []
        for i in range(1, total_row):  # 从第2行开始遍历
            row = sheet_1.row(i)
            if row is None:
                break
            date = str(date).format("'%Y%m%d'").replace('-', '')  # 信用交易日期
            bdzjdm = str(row[0].value).replace(",", "")  # 标的证劵代码
            bdzjjc = str(row[1].value).replace(",", "")  # 标的证劵简称
            rzye = float(str(row[2].value).replace(",", ""))  # 融资余额(元)
            rzmre = float(str(row[3].value).replace(",", ""))  # 融资买入额(元)
            rzche = float(str(row[4].value).replace(",", ""))  # 融资偿还额(元)
            rjyl = str(row[5].value).replace(",", "")  # 融券余量
            rjmcl = str(row[6].value).replace(",", "")  # 融券卖出量
            rjchl = str(row[7].value).replace(",", "")  # 融资偿还量
            data_list.append((date, bdzjdm, bdzjjc, rzye, rzmre, rzche, rjyl, rjmcl, rjchl))

        logger.info("excel处理完成，开始处理详细数据")
        return data_list, total_row
    except Exception as e:
        raise Exception(e)


if __name__ == '__main__':
    collector = CollectHandler()
    if len(sys.argv) > 1:
        collector.collect_data(eval(sys.argv[1]))
    else:
        logger.error(f'business_type为必传参数')
