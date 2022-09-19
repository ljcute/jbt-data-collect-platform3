#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/6/27 09:33
# 上海交易所-融资融券可充抵保证金证券及融资/融券标的证券

import os
import sys
import traceback
from configparser import ConfigParser


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)


from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder
from utils.remove_file import remove_file

import json
import time
import xlrd2
import os
import datetime
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'sz_exchange_mt_underlying_and_guaranty_security.xlsx')

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

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

data_source_szse = '深圳交易所'
data_source_sse = '上海交易所'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, query_date=None):
        max_retry = 0
        while max_retry < 5:
            logger.info(f'重试第{max_retry}次')
            actual_date = datetime.date.today() if query_date is None else query_date
            logger.info(f'上交所数据采集开始{actual_date}')
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
            try:
                proxies = super().get_proxies()
                title_list = ['biz_dt', 'sec_code', 'sec_name']

                # 担保券
                log_message_gu = "上交所担保券"
                cls.deal_with(proxies, headers, url_guaranty, sh_guaranty_file_path, save_excel_file_path_gu,
                              exchange_mt_guaranty_security, data_source_sse, title_list, actual_date, log_message_gu, max_retry)

                # 融资标的券
                log_message_rz = "上交所融资标的券"
                cls.deal_with(proxies, headers, url_rz, sh_target_rz_file_path, save_excel_file_path_rz,
                              exchange_mt_financing_underlying_security, data_source_sse, title_list, actual_date,
                              log_message_rz, max_retry)

                # 融券标的券
                log_message_rq = "上交所融券标的券"
                cls.deal_with(proxies, headers, url_rq, sh_target_rq_file_path, save_excel_file_path_rq,
                              exchange_mt_lending_underlying_security, data_source_sse, title_list, actual_date,
                              log_message_rq, max_retry)

                logger.info("上交所融资融券数据采集完成")
                break
            except ProxyTimeOutEx as es:
                pass
            except Exception as e:
                time.sleep(3)
                logger.error(f'{data_source_sse}标的券及担保券数据采集任务出现异常，请求url为：{url_guaranty}，输入参数为：{actual_date}，具体异常信息为:{traceback.format_exc()}')
            finally:
                remove_file(sh_guaranty_file_path)
                remove_file(sh_target_rz_file_path)
                remove_file(sh_target_rq_file_path)

            max_retry += 1

    @classmethod
    def deal_with(cls, proxies, headers, url, excel_path, save_excel_path, data_type, data_source, title_list,
                  actual_date, log_message, max_retry):
        start_dt = datetime.datetime.now()

        try:
            response = super().get_response(data_source, url, proxies, 0, headers)
            data = None
            if response is None or response.status_code != 200:
                raise Exception(f'{data_source}数据采集任务请求响应获取异常,已获取代理ip为:{proxies}，请求url为:{url},请求参数为:{data}')
            download_excel(response, excel_path, save_excel_path, actual_date)
            logger.info(f'{log_message}开始采集')
            excel_file = xlrd2.open_workbook(excel_path, encoding_override="utf-8")
            data_list, total_row = handle_excel(excel_file, actual_date)
            df_result = super().data_deal(data_list, title_list)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if int(len(data_list)) == total_row - 1:
                data_statust = 1
                super().data_insert(int(len(data_list)), df_result, actual_date, data_type,
                                    data_source, start_dt, end_dt, used_time, url, data_statust, save_excel_path)
                logger.info(f'{log_message}入库信息,共{int(len(data_list))}条')
            elif int(len(data_list)) != total_row - 1:
                data_statust = 2
                super().data_insert(int(len(data_list)), df_result, actual_date, data_type,
                                    data_source, start_dt, end_dt, used_time, url, data_statust, save_excel_path)
                logger.info(f'{log_message}入库信息,共{int(len(data_list))}条')

            message_gu = log_message + "数据采集完成"
            message = 'sh_exchange_mt_underlying_and_guaranty_security'
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      data_type, data_source_sse, message)
        except Exception as e:
            if max_retry == 4:
                data_statust = 2
                super().data_insert(0, str(e), actual_date, data_type,
                                    data_source, start_dt, None, None, url, data_statust, save_excel_path)

            raise Exception(e)


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
        return data_list, total_row

    except Exception as es:
        raise Exception(es)


if __name__ == '__main__':
    collector = CollectHandler()
    if len(sys.argv) > 1:
        collector.collect_data(sys.argv[1])
    else:
        collector.collect_data()