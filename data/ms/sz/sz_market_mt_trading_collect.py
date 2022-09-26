#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/24 13:33
# 深圳交易所-市场融资融券交易总量/市场融资融券交易明细

import os
import sys
import time
import traceback
from configparser import ConfigParser

from selenium.webdriver.common.by import By

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder
from utils.remove_file import remove_file, random_double
import json
import datetime
import xlrd2
from constants import USER_AGENTS
import random
import os
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sz_balance.xlsx')
excel_file_path_anthoer = os.path.join(base_dir, 'sz_balance_total.xlsx')

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, "深交所融资融券{}.xlsx".format(datetime.date.today()))
save_excel_file_path_total = os.path.join(paths, "深交所融资融券交易总量{}.xlsx".format(datetime.date.today()))


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '深圳交易所'

    def trading_amount_collect(self):
        trade_date = self.get_trade_date()
        self.biz_dt = trade_date
        self.url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1837_xxpl&TABKEY=tab1'
        headers = {
            'User-Agent': random.choice(USER_AGENTS)
        }
        params = {
            'txtDate': trade_date,  # 查历史可以传日期
            'random': random_double(),
        }
        response = self.get_response(self.url, 0, headers, params)
        self.data_list, total_num = self.total_deal(response, trade_date)
        self.total_num = total_num - 1
        self.collect_num = len(self.data_list)

    def total_deal(self, response, actual_date):
        try:
            try:
                logger.info("开始下载excel")
                with open(excel_file_path_anthoer, 'wb') as file:
                    file.write(response.content)
                with open(save_excel_file_path_total, 'wb') as file:
                    file.write(response.content)
                logger.info("excel下载完成")
            except Exception as e:
                raise Exception(e)

            excel_file = xlrd2.open_workbook(excel_file_path_anthoer, encoding_override="utf-8")
            data_list, total = self.handle_excel_total(excel_file, actual_date)
            return data_list, total
        except Exception as e:
            raise Exception(e)
        finally:
            remove_file(excel_file_path_anthoer)

    def handle_excel_total(self, excel_file, actual_date):
        logger.info("开始处理excel")
        sheet_0 = excel_file.sheet_by_index(0)
        total_row = sheet_0.nrows
        data_list = []
        if total_row >= 1:
            for i in range(1, total_row):
                row = sheet_0.row(i)
                if row is None:
                    break

                jrrzye = str(row[1].value).replace(",", "")  # 融资余额(亿元)
                jrrjye = str(row[4].value).replace(",", "")  # 融券余额(亿元)
                jrrzrjye = str(row[5].value).replace(",", "")  # 融资融券余额(亿元)
                jrrzmr = str(row[0].value).replace(",", "")  # 融资买入额(亿元)
                jrrjmc = str(row[2].value).replace(",", "")  # 融券卖出量(亿股/亿份)
                jrrjyl = str(row[3].value).replace(",", "")  # 融券余量(亿股/亿份)
                data_list.append((jrrzye, jrrjye, jrrzrjye, jrrzmr, jrrjmc, jrrjyl))

            logger.info("excel处理结束")
            return data_list, total_row

    def trading_items_collect(self):
        trade_date = self.get_trade_date()
        self.biz_dt = trade_date
        self.url = "https://www.szse.cn/api/report/ShowReport"
        headers = {
            'User-Agent': random.choice(USER_AGENTS)
        }
        params = {
            'SHOWTYPE': 'xlsx',
            'CATALOGID': '1837_xxpl',
            'txtDate': trade_date,  # 查历史可以传日期
            'random': random_double(),
            'TABKEY': 'tab2'
        }
        response = self.get_response(self.url, 0, headers, params)
        self.data_list, total_num = self.item_deal(response, trade_date)
        self.collect_num = self.total_num = len(self.data_list)

    def item_deal(self, response, actual_date):
        try:
            try:
                logger.info("开始下载excel")
                with open(excel_file_path, 'wb') as file:
                    file.write(response.content)
                with open(save_excel_file_path, 'wb') as file:
                    file.write(response.content)
                logger.info("excel下载完成")
            except Exception as e:
                raise Exception(e)

            excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
            data_list, total_row = self.handle_excel(excel_file, actual_date)
            return data_list, total_row
        except Exception as e:
            raise Exception(e)
        finally:
            remove_file(excel_file_path)

    def get_trade_date(self):
        url = 'http://www.szse.cn/disclosure/margin/margin/index.html'
        try:
            logger.info(f'开始获取深圳交易所最新交易日日期')
            driver = super().get_driver()
            driver.get(url)
            time.sleep(3)
            trade_date = \
                driver.find_elements(By.XPATH,
                                     '/html/body/div[5]/div/div[2]/div/div/div[4]/div[1]/div[1]/div[1]/span[2]')[
                    0].text
            logger.info(f'深圳交易所最新交易日日期为{trade_date}')
            return trade_date

        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            raise Exception(f'获取深圳交易所最新交易日日期异常，请求url为：{url}，具体异常信息为：{e}')

    def handle_excel(self, excel_file, actual_date):
        logger.info("开始处理excel")
        sheet_0 = excel_file.sheet_by_index(0)
        total_row = sheet_0.nrows
        if total_row >= 1:
            # try:
            data_list = []
            for i in range(1, total_row):  # 从第2行开始遍历
                row = sheet_0.row(i)
                if row is None:
                    break

                zqdm = str(row[0].value).replace(",", "")  # 证券代码
                zqjc = str(row[1].value).replace(",", "")  # 证券简称
                jrrzye = str(row[3].value).replace(",", "")  # 融资余额(元)
                jrrzmr = str(row[2].value).replace(",", "")  # 融资买入额(元)
                jrrjyl = str(row[5].value).replace(",", "")  # 融券余量(股/份)
                jrrjye = str(row[6].value).replace(",", "")  # 融券余额(元)
                jrrjmc = str(row[4].value).replace(",", "")  # 融券卖出量(股/份)
                jrrzrjye = str(row[7].value).replace(",", "")  # 融资融券余额(亿元)
                data_list.append((zqdm, zqjc, jrrzye, jrrzmr, jrrjyl, jrrjye, jrrjmc, jrrzrjye))

            logger.info("excel处理结束")
            return data_list, total_row


if __name__ == "__main__":
    collector = CollectHandler()
    if len(sys.argv) > 1:
        collector.collect_data(eval(sys.argv[1]))
    else:
        logger.error(f'business_type为必传参数')