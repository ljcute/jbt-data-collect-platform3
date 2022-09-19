#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/12 17:05
# @Site    :
# @Software: PyCharm
# 上海交易所-融资融券交易汇总及详细数据
import os
import sys
import json
import time
import traceback

import xlrd2
import datetime
import os
from configparser import ConfigParser

from selenium.webdriver.common.by import By


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from utils.deal_date import ComplexEncoder
from utils.logs_utils import logger
from data.ms.basehandler import BaseHandler
from utils.remove_file import remove_file

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sh_balance.xls')

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, "上交所融资融券{}.xls".format(datetime.date.today()))

data_type_market_mt_trading_amount = '0'  # 市场融资融券交易总量
data_type_market_mt_trading_items = '1'  # 市场融资融券交易明细

data_source_szse = '深圳交易所'
data_source_sse = '上海交易所'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, query_date=None):
        max_retry = 0
        while max_retry < 5:
            logger.info(f'重试第{max_retry}次')
            start_dt = datetime.datetime.now()
            actual_date = datetime.date.today() if query_date is None else query_date
            trade_date = cls.get_trade_date() if query_date is None else query_date
            logger.info(f'上交所最新交易日期==================为{trade_date}')
            logger.info(f'上交所数据采集开始{actual_date}')
            download_excel_url = "http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk20220623.xls"
            replace_str = 'rzrqjygk' + str(trade_date).format("'%Y%m%d'").replace('-', '') + '.xls'
            download_excel_url = download_excel_url.replace(download_excel_url.split('/')[-1], replace_str)
            try:
                proxies = super().get_proxies()
                response = super().get_response(data_source_sse, download_excel_url, proxies, 0)
                data = None
                if response is None or response.status_code != 200:
                    raise Exception(f'{data_source_sse}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{download_excel_url},请求参数为:{data}')
                download_excel(response, actual_date)
                logger.info("excel下载完成，开始处理excel")
                excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
                title_list = ['date', 'rzye', 'rzmre', 'rjyl', 'rjylje', 'rjmcl', 'rzrjye']
                title_list_detail = ['date', 'bdzjdm', 'bdzjjc', 'rzye', 'rzmre', 'rzche', 'rjyl', 'rjmcl', 'rjchl']

                data_list, total_row = handle_excel_total(excel_file, trade_date)
                df_result = super().data_deal(data_list, title_list)
                end_dt = datetime.datetime.now()
                used_time = (end_dt - start_dt).seconds
                logger.info(f'开始入库汇总信息,共{int(len(data_list))}条')
                if int(len(data_list)) == total_row - 17:
                    data_status = 1
                    super().data_insert(int(len(data_list)), df_result, trade_date, data_type_market_mt_trading_amount,
                                        data_source_sse, start_dt, end_dt, used_time, download_excel_url, data_status,
                                        save_excel_file_path)
                    logger.info(f'入库信息：{int(len(data_list))}条')

                elif int(len(data_list)) != total_row - 17:
                    data_status = 2
                    super().data_insert(int(len(data_list)), df_result, trade_date, data_type_market_mt_trading_amount,
                                        data_source_sse, start_dt, end_dt, used_time, download_excel_url, data_status,
                                        save_excel_file_path)
                    logger.info(f'入库信息：{int(len(data_list))}条')

                message = "sh_market_mt_trading_collect"
                super().kafka_mq_producer(json.dumps(trade_date, cls=ComplexEncoder),
                                          data_type_market_mt_trading_amount, data_source_sse, message)

                data_list_detail, total_row_detail = handle_excel_detail(excel_file, trade_date)
                df_result_detail = super().data_deal(data_list_detail, title_list_detail)
                end_dt_detal = datetime.datetime.now()
                used_time_detail = (end_dt_detal - start_dt).seconds
                logger.info(f'开始入库详细信息,共{int(len(data_list_detail))}条')
                if int(len(data_list_detail)) == total_row_detail - 1:
                    data_status = 1
                    super().data_insert(int(len(data_list_detail)), df_result_detail, trade_date,
                                        data_type_market_mt_trading_items, data_source_sse, start_dt, end_dt_detal,
                                        used_time_detail,
                                        download_excel_url, data_status, save_excel_file_path)
                elif int(len(data_list_detail)) == total_row_detail - 1:
                    data_status = 2
                    super().data_insert(int(len(data_list_detail)), df_result_detail, trade_date,
                                        data_type_market_mt_trading_items, data_source_sse, start_dt, end_dt_detal,
                                        used_time_detail,
                                        download_excel_url, data_status, save_excel_file_path)

                logger.info(f'上交所数据采集结束{datetime.date.today()}')
                message_1 = "sh_market_mt_trading_collect"
                super().kafka_mq_producer(json.dumps(trade_date, cls=ComplexEncoder),
                                          data_type_market_mt_trading_items, data_source_sse,
                                          message_1)

                break
            except ProxyTimeOutEx as es:
                pass
            except Exception as e:
                time.sleep(3)
                logger.error(f'{data_source_sse}交易明细及汇总数据采集任务出现异常，请求url为：{download_excel_url}，输入参数为：{trade_date}，具体异常信息为:{traceback.format_exc()}')
                if max_retry == 4:
                    data_status = 2
                    super().data_insert(0, str(e), actual_date, data_type_market_mt_trading_items,
                                        data_source_sse, start_dt, None, None, download_excel_url, data_status)
            finally:
                remove_file(excel_file_path)

            max_retry += 1

    @classmethod
    def get_trade_date(cls):
        try:
            logger.info(f'开始获取上海交易所最新交易日日期')
            driver = super().get_driver()
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
        collector.collect_data(sys.argv[1])
    else:
        collector.collect_data()
