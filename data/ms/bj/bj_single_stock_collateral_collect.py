#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yexy
# @Time    : 2023/6/08 16:00
# @Site    :
# @File    : bj_single_stock_collateral_collect.py
# @Software: PyCharm
# 北京交易所-单一股票担保物比例信息 https://www.bse.cn/disclosure/rzrq_dygpdbwbl.html
import sys
import datetime
import os
import pandas as pd
import warnings
from configparser import ConfigParser

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler, argv_param_invoke, logger
from constants import get_headers
from utils.deal_date import last_work_day

base_dir = os.path.dirname(os.path.abspath(__file__))
excel_file_path = os.path.join(base_dir, 'bj_balance.xls')

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
paths = cf.get('excel-path', 'save_excel_file_path')
save_excel_file_path = os.path.join(paths, "北交所融资融券{}.xls".format(datetime.date.today()))

class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '北京交易所'
        self.collect_num_check = False
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'yfx_c_g_u_id_10000042=_ck22060711191911485514723010297; '
                      'VISITED_MENU=%5B%228307%22%2C%229729%22%5D; JSESSIONID=771FCD96DF812328467D7B327B093D35; '
                      'gdp_user_id=gioenc-6e004388%2C3d26%2C59c4%2C838g%2C4063ea3a9528; '
                      'ba17301551dcbaf9_gdp_session_id=4a6d84c6-2cd3-4b35-b7eb-4286992ff745; '
                      'ba17301551dcbaf9_gdp_session_id_4a6d84c6-2cd3-4b35-b7eb-4286992ff745=true; '
                      'yfx_f_l_v_t_10000042=f_t_1654571959111__r_t_1655691628385__v_t_1655692038721__r_c_5',
            'Host': 'www.bse.cn',
            'Referer': 'https://www.bse.cn/disclosure/rzrq_dygpdbwbl.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36'
        }
        self.trade_date = last_work_day(self.search_date)

    # def single_stock_collateral_collect(self):
    #     if isinstance(param_dt, str):
    #         logger.info(f'param_dt:{param_dt, type(param_dt)}')
    #         self.trade_date = datetime.datetime.strptime(param_dt, '%Y%m%d').strftime('%Y-%m-%d')
    #         logger.info(f'进入查询北交所历史交易数据分支!')
    #     self.url = f"https://www.bse.cn/dygpdbwblController/export.do?zqdm=&transDate={self.trade_date}"
    #     response = self.get_response(self.url, 1, self.headers)#实际是GET请求，但是POST才请求成功
    #     warnings.filterwarnings('ignore')
    #     df = pd.read_excel(response.content, header=0)
    #     if not df.empty:
    #         # 日期处理
    #         dt_str = df.values[-1][0]
    #         if '日期' in dt_str:
    #             dt = dt_str.replace('日期', '').replace('：', '')
    #             df.drop([len(df)-1], inplace=True)
    #             df['日期'] = dt
    #         self.total_num = df.index.size
    #         self.collect_num = self.total_num
    #         self.data_text = df.to_csv(index=False)


    def single_stock_collateral_collect(self):
        logger.info(f"开始爬取北京交易所-单一股票担保物比例信息:{datetime.datetime.now()}")
        page = 0
        self.url = f'https://www.bse.cn/dygpdbwblController/infoResult.do?callback=jQuery331_1686213639369'
        if isinstance(param_dt, str):
            logger.info(f'param_dt:{param_dt, type(param_dt)}')
            logger.info(f'进入查询北交所历史交易数据分支!')
            self.trade_date = datetime.datetime.strptime(param_dt, '%Y%m%d').strftime('%Y-%m-%d')
            data = {
                "transDate": self.trade_date,
                "page": page,
                "zqdm": None,
                "sortfield": None,
                "sorttype": None
            }
        else:
            data = {
                "transDate": None,
                "page": page,
                "zqdm": None,
                "sortfield": None,
                "sorttype": None
            }
        response = self.get_response(self.url, 1, get_headers(), data=data)
        temp_text = response.text
        s = temp_text[temp_text.index('['):]
        s = s[:-1]
        text = eval(s.replace('true', 'True').replace('false', 'False').replace('null', 'None'))
        total_pages = text[0][0]['totalPages']
        self.total_num = text[0][0]['totalElements']
        for i in range(0, total_pages):
            if isinstance(param_dt, str):
                logger.info(f'param_dt:{param_dt, type(param_dt)}')
                logger.info(f'进入查询北交所历史交易数据分支!')
                data_ = {
                    "transDate": self.trade_date,
                    "page": i,
                    "sortfield": None,
                    "sorttype": None
                }
            else:
                data_ = {
                    "transDate": None,
                    "page": i,
                    "sortfield": None,
                    "sorttype": None
                }
            response_ = self.get_response(self.url, 1, get_headers(), data=data_)
            temp_text_ = response_.text
            s_ = temp_text_[temp_text_.index('['):]
            s_ = s_[:-1]
            text_ = eval(s_.replace('true', 'True').replace('false', 'False').replace('null', 'None'))
            data_list_ = text_[0][0]['content']
            self.tmp_df = pd.concat([self.tmp_df, pd.DataFrame(data_list_)])

        self.tmp_df['业务日期'] = text[1]
        if not self.tmp_df.empty:
            self.collect_num = self.tmp_df.index.size
            self.data_text = self.tmp_df.to_csv(index=False)
        else:
            self.data_status = 3


if __name__ == '__main__':
    argv = sys.argv
    param_dt = None
    if len(argv) == 3:
        param_dt = argv[2]
    argv_param_invoke(CollectHandler(), (6,), sys.argv)
    #CollectHandler().collect_data(6)
