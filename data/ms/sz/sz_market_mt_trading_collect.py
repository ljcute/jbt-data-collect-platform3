#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/24 13:33
# 深圳交易所-市场融资融券交易总量/市场融资融券交易明细

import os
import sys
import time
from configparser import ConfigParser

from selenium.webdriver.common.by import By

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

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

data_type_market_mt_trading_amount = '0'  # 市场融资融券交易总量
data_type_market_mt_trading_items = '1'  # 市场融资融券交易明细

data_source_szse = '深圳交易所'
data_source_sse = '上海交易所'
broker_id = 1000092


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, query_date=None):
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            # try:
            cls.total(query_date)
            # cls.item(query_date)
            logger.info("深交所交易汇总及详细数据采集完成")
            break
            # except Exception as e:
            time.sleep(3)
            logger.error(e)

            max_retry += 1

    @classmethod
    def get_trade_date(cls):
        try:
            logger.info(f'开始获取深圳交易所最新交易日日期')
            driver = super().get_driver()
            url = 'http://www.szse.cn/disclosure/margin/margin/index.html'
            driver.get(url)
            time.sleep(3)
            trade_date = driver.find_elements(By.XPATH, '/html/body/div[5]/div/div[2]/div/div/div[4]/div[1]/div[1]/div[1]/span[2]')[0].text
            logger.info(f'深圳交易所最新交易日日期为{trade_date}')
            return trade_date
        except Exception as e:
            logger.error(e)

    @classmethod
    def total(cls, query_date):
        actual_date = datetime.date.today() if query_date is None else query_date
        trade_date = cls.get_trade_date() if query_date is None else query_date
        logger.info(f'深交所汇总数据采集开始{actual_date}')
        logger.info(f'深圳交易所最新交易日日期为{trade_date}')

        url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1837_xxpl&TABKEY=tab1'
        headers = {
            'User-Agent': random.choice(USER_AGENTS)
        }
        params = {
            'txtDate': trade_date,  # 查历史可以传日期
            'random': random_double(),
        }
        # ss_url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1837_xxpl&TABKEY=tab1&txtDate=2022-08-26&random=0.6332194803268989'
        proxies = super().get_proxies()
        title_list = ['jrrzye', 'jrrjye', 'jrrzrjye', 'jrrzmr', 'jrrjmc', 'jrrjyl']
        start_dt = datetime.datetime.now()
        response = super().get_response(url, proxies, 0, headers, params)
        data_list = cls.total_deal(response, trade_date)
        logger.info(f'data_list:{data_list}')
        df_result = super().data_deal(data_list, title_list)
        logger.info(f'df_result:{df_result}')
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        if data_list:
            super().data_insert(int(len(data_list)), df_result, trade_date, data_type_market_mt_trading_amount,
                                data_source_szse, start_dt, end_dt, used_time, url)
            logger.info(f'数据入库信息,共{int(len(data_list))}条')
        else:
            raise Exception(f'采集数据失败，为{int(len(data_list))}条，需要重新采集')
        message = "sz_market_mt_trading_collect"
        super().kafka_mq_producer(json.dumps(trade_date, cls=ComplexEncoder),
                                  data_type_market_mt_trading_amount, data_source_szse, message)

    @classmethod
    def total_deal(cls, response, actual_date):
        try:
            try:
                logger.info("开始下载excel")
                with open(excel_file_path_anthoer, 'wb') as file:
                    file.write(response.content)
                with open(save_excel_file_path_total, 'wb') as file:
                    file.write(response.content)
                logger.info("excel下载完成")
            except Exception as e:
                logger.error(e)

            excel_file = xlrd2.open_workbook(excel_file_path_anthoer, encoding_override="utf-8")
            data_list, total_row = cls.handle_excel_total(excel_file, actual_date)
            return data_list, total_row
        except Exception as e:
            logger.error(e)
        finally:
            remove_file(excel_file_path_anthoer)

    @classmethod
    def handle_excel_total(cls, excel_file, actual_date):
        logger.info("开始处理excel")
        sheet_0 = excel_file.sheet_by_index(0)
        total_row = sheet_0.nrows
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
                data_list = [jrrzye, jrrjye, jrrzrjye, jrrzmr, jrrjmc, jrrjyl]

            logger.info("excel处理结束")
            return data_list, total_row
        else:
            logger.error(f'该查询日期{actual_date}暂无相关交易数据！')


    @classmethod
    def item(cls, query_date):
        actual_date = datetime.date.today() if query_date is None else query_date
        trade_date = cls.get_trade_date() if query_date is None else query_date

        logger.info(f'深交所详细数据采集开始{actual_date}')
        download_url = "https://www.szse.cn/api/report/ShowReport"
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
        proxies = super().get_proxies()
        title_list = ['zqdm', 'zqjc', 'jrrzye', 'jrrzmr', 'jrrjyl', 'jrrjye', 'jrrjmc', 'jrrzrjye']
        start_dt = datetime.datetime.now()
        response = super().get_response(download_url, proxies, 0, headers, params)
        data_list, total_row = cls.item_deal(response, actual_date)
        df_result = super().data_deal(data_list, title_list)
        logger.info(f'df_result:{df_result}')
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        if int(len(data_list)) == total_row - 1:
            super().data_insert(int(len(data_list)), df_result, trade_date, data_type_market_mt_trading_items,
                                data_source_szse, start_dt, end_dt, used_time, download_url)
            logger.info(f'数据入库信息,共{int(len(data_list))}条')
        else:
            raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{total_row - 1}不一致，入库失败')
        message = "sz_market_mt_trading_collect"
        super().kafka_mq_producer(json.dumps(trade_date, cls=ComplexEncoder),
                                  data_type_market_mt_trading_items, data_source_szse, message)

    @classmethod
    def item_deal(cls, response, actual_date):
        try:
            try:
                logger.info("开始下载excel")
                with open(excel_file_path, 'wb') as file:
                    file.write(response.content)
                with open(save_excel_file_path, 'wb') as file:
                    file.write(response.content)
                logger.info("excel下载完成")
            except Exception as e:
                logger.error(e)

            excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
            data_list, total_row = cls.handle_excel(excel_file, actual_date)
            return data_list, total_row
        except Exception as e:
            logger.error(e)
        finally:
            remove_file(excel_file_path)

    @classmethod
    def handle_excel(cls, excel_file, actual_date):
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
            # except Exception as es:
            #     logger.error(es)
        else:
            logger.error(f'该查询日期{actual_date}暂无相关交易数据！')


# def collect_history(begin_dt, end_dt):
#     # begin = datetime.datetime.strptime('20210605', '%Y%m%d')
#     begin = datetime.datetime.strptime(begin_dt, '%Y%m%d')
#     end = datetime.datetime.strptime(end_dt, '%Y%m%d')
#     b = begin.date()
#     e = end.date()
#
#     for k in range((e - b).days + 1):
#         cur_date = b + datetime.timedelta(days=k)
#         collect(cur_date)


if __name__ == "__main__":
    collector = CollectHandler()
    # collector.collect_data('2022-07-12')
    if len(sys.argv) > 1:
        collector.collect_data(sys.argv[1])
    else:
        collector.collect_data()
