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
import xlrd2
import datetime
import os
from configparser import ConfigParser

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.deal_date import ComplexEncoder
from utils.logs_utils import logger
from data.ms.basehandler import BaseHandler
from utils.remove_file import remove_file

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sh_balance.xls')

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path)
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, '上交所融资融券.xls')

data_type_market_mt_trading_amount = '0'  # 市场融资融券交易总量
data_type_market_mt_trading_items = '1'  # 市场融资融券交易明细

data_source_szse = '深圳交易所'
data_source_sse = '上海交易所'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, query_date=None):
        max_retry = 0
        while max_retry < 3:
            try:
                start_dt = datetime.datetime.now()
                actual_date = datetime.date.today() if query_date is None else query_date
                logger.info(f'上交所数据采集开始{actual_date}')
                download_excel_url = "http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk20220623.xls"
                if query_date is not None:
                    replace_str = 'rzrqjygk' + str(query_date).format("'%Y%m%d'").replace('-', '') + '.xls'
                    download_excel_url = download_excel_url.replace(download_excel_url.split('/')[-1], replace_str)
                proxies = super().get_proxies()
                response = super().get_response(download_excel_url, proxies, 0)
                download_excel(response, actual_date)
                logger.info("excel下载完成，开始处理excel")
                excel_file = xlrd2.open_workbook(excel_file_path, encoding_override="utf-8")
                title_list = ['date', 'rzye', 'rzmre', 'rjyl', 'rjylje', 'rjmcl', 'rzrjye']
                title_list_detail = ['date', 'bdzjdm', 'bdzjjc', 'rzye', 'rzmre', 'rzche', 'rjyl', 'rjmcl', 'rjchl']

                data_list, total_row = handle_excel_total(excel_file, actual_date)
                df_result = super().data_deal(data_list, title_list)
                end_dt = datetime.datetime.now()
                used_time = (end_dt - start_dt).seconds
                logger.info(f'开始入库汇总信息,共{int(len(data_list))}条')
                if int(len(data_list)) == total_row - 17:
                    super().data_insert(int(len(data_list)), df_result, actual_date, data_type_market_mt_trading_amount,
                                        data_source_sse, start_dt, end_dt, used_time, download_excel_url,
                                        save_excel_file_path)
                else:
                    raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{total_row - 1}不一致，入库失败')
                message = "上海交易所融资融券交易汇总数据采集完成"
                super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                          data_type_market_mt_trading_amount, data_source_sse, message)

                data_list_detail, total_row_detail = handle_excel_detail(excel_file, actual_date)
                df_result_detail = super().data_deal(data_list_detail, title_list_detail)
                end_dt_detal = datetime.datetime.now()
                used_time_detail = (end_dt_detal - start_dt).seconds
                logger.info(f'开始入库详细信息,共{int(len(data_list_detail))}条')
                if int(len(data_list_detail)) == total_row_detail - 1:
                    super().data_insert(int(len(data_list_detail)), df_result_detail, actual_date,
                                        data_type_market_mt_trading_items, data_source_sse, start_dt, end_dt_detal,
                                        used_time_detail,
                                        download_excel_url, save_excel_file_path)
                else:
                    raise Exception(f'采集数据条数{int(len(data_list_detail))}与官网数据条数{total_row_detail - 1}不一致，入库失败')
                logger.info(f'上交所数据采集结束{datetime.date.today()}')
                message_1 = "上海交易所融资融券交易详细数据采集完成"
                super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                          data_type_market_mt_trading_items, data_source_sse,
                                          message_1)

                break
            except Exception as e:
                time.sleep(3)
                logger.error(e)
            finally:
                remove_file(excel_file_path)

            max_retry += 1


def download_excel(response, query_date=None):
    if response.status_code == 200:
        try:
            with open(excel_file_path, 'wb') as file:
                file.write(response.content)  # 写excel到当前目录
            with open(save_excel_file_path, 'wb') as file:
                file.write(response.content)  # 写excel到当前目录
        except Exception as es:
            logger.error(es)
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
        logger.error(e)


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
        logger.error(e)


if __name__ == '__main__':
    collector = CollectHandler()
    collector.collect_data()
