#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/12/20 11:10
# @Site    : 
# @File    : pa_securities_collect.py
# @Software: PyCharm
import os
import sys
import time
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, get_headers, logger, argv_param_invoke
from selenium.webdriver.common.by import By



class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '平安证券'
        self.init_date = None
        self.url = ''
        self._proxies = self.get_proxies()
        self.total_page = 0

    def rzrq_underlying_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('bd')

    def guaranty_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('db')

    def _securities_collect(self, biz_type):
        if biz_type == 'bd':
            self.url = "https://stock.pingan.com/static/webinfo/margin/business.html?businessType=0"
        elif biz_type == 'db':
            self.url = "https://stock.pingan.com/static/webinfo/margin/business.html?businessType=1"
        try:
            driver = self.get_driver()
            driver.get(self.url)
            driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
            time.sleep(3)
            # 找到券商官网写的总页数
            span_element = driver.find_elements(By.CSS_SELECTOR, "#business-query-box > div > a:nth-child(7)")
            self.total_page = int(span_element[0].text)
            # 当前网页内容(第1页)
            html_content = str(driver.page_source)
            _df1 = pd.read_html(html_content)[0]
            _df1.sort_values(by=['证券代码', '证券简称'], ascending=[True, True])
            self.tmp_df = pd.concat([self.tmp_df, _df1])
            self.collect_num = self.tmp_df.index.size
            current_page = 1
            logger.info(f"平安证券标的券第{current_page}/{self.total_page}页，记录数{self.collect_num}/{self.total_num}条")
            while current_page < self.total_page:
                # 找到下一页 >按钮
                driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
                driver.implicitly_wait(10)
                elements = driver.find_elements(By.CSS_SELECTOR, "#business-query-box > div > a.nextPage.page")
                driver.implicitly_wait(10)
                next_page_button_element = elements[0]

                if not next_page_button_element.is_selected():
                    driver.execute_script('arguments[0].click();', next_page_button_element)
                    driver.implicitly_wait(10)
                    _df2 = pd.read_html(html_content)[0]
                    _df2.sort_values(by=['证券代码', '证券简称'], ascending=[True, True])
                    # 读取内容与上一次完全相同，则休息一下再重新读取内容
                    counter = 0
                    while _df1.equals(_df2):
                        logger.info(f'平安证券标的券采集速度过快，休息一下{counter}')
                        time.sleep(0.01)
                        html_content = str(driver.page_source)
                        _df2 = pd.read_html(html_content)[0]
                        _df2.sort_values(by=['证券代码', '证券简称'], ascending=[True, True])
                        if counter > 100:
                            driver.quit()
                            msg = f"平安证券标的券采集速度过快,连续休息100次重试还是存在问题!"
                            logger.info(msg)
                            raise Exception(msg)
                        counter += 1
                    _df1 = _df2
                    self.tmp_df = pd.concat([self.tmp_df, _df1])
                    # 与上一次存在重复，否则需要整个重采
                    if self.tmp_df.duplicated(['证券代码', '证券简称']).sum() > 0:
                        dep_line = self.tmp_df[
                            self.tmp_df.duplicated(['证券代码', '证券简称'], keep='last')]  # 查看删除重复的行
                        dep_list = dep_line.values.tolist()
                        msg = f'平安证券标的券采集的重复数据为：{dep_list},共{len(dep_list)}条'
                        logger.info(msg)
                        raise Exception(msg)
                    self.collect_num = self.tmp_df.index.size
                    logger.info(
                        f"平安证券标的券第{current_page + 1}/{self.total_page}页，记录数{self.collect_num}/{self.total_num}条")
                current_page += 1
            self.data_text = self.tmp_df.to_csv(index=False)
            if self.collect_num > (int(self.total_page) - 1) * 10:
                self.total_num = self.collect_num
        finally:
            driver.quit()





if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
