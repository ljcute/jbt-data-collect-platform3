#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/28 15:16
import os
import sys
import time
import pandas as pd
from selenium.webdriver.common.by import By

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, logger, argv_param_invoke


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '申万宏源'
        self.total_page = 0

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.swhysc.com/swhysc/financial/marginTradingList?channel=00010017000300020001&listId=2'
        self._securities_collect('bd')

    def guaranty_securities_collect(self):
        self.url = 'https://www.swhysc.com/swhysc/financial/marginTradingList?channel=00010017000300020001&listId=1'
        self._securities_collect('db')

    def _securities_collect(self, biz_type):
        try:
            driver = self.get_driver()
            driver.get(self.url)
            driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
            time.sleep(3)
            # 找到券商官网写的总条数
            span_element = driver.find_elements(By.XPATH, "//*[@id='root']/section/div[2]/div/div[3]/div/div[1]/form/div[1]/div/div/div/span")
            self.total_num = int(span_element[0].text)
            # 当前网页内容(第1页)
            html_content = str(driver.page_source)
            _df1 = pd.read_html(html_content)[0]
            _df1.sort_values(by=['市场', '证券代码', '证券简称'], ascending=[True, True, True])
            self.tmp_df = pd.concat([self.tmp_df, _df1])
            self.collect_num = self.tmp_df.index.size
            # 找到总页数
            if biz_type == 'db':
                li_elements = driver.find_elements(By.XPATH, "//*[@id='root']/section/div[2]/div/div[3]/div/div[2]/div/div/div/ul/li[8]/a")
            elif biz_type == 'bd':
                li_elements = driver.find_elements(By.XPATH, "//li[contains(@class, 'ant-pagination-item')]")
            else:
                raise Exception(f'业务类型(biz_type): {biz_type}错误')
            if len(li_elements) > 0:
                self.total_page = int(li_elements[-1].text)
            current_page = 1
            logger.info(f"申万宏源担保券第{current_page}/{self.total_page}页，记录数{self.collect_num}/{self.total_num}条")
            while current_page < self.total_page:
                # 找到下一页 >按钮
                driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
                driver.implicitly_wait(10)
                elements = driver.find_elements(By.XPATH, "//button[@class='ant-pagination-item-link']")
                driver.implicitly_wait(10)
                next_page_button_element = elements[-1]
                if not next_page_button_element.is_selected():
                    driver.execute_script('arguments[0].click();', next_page_button_element)
                    driver.implicitly_wait(10)
                    # 处理第[2, total_page]页html
                    html_content = str(driver.page_source)
                    _df2 = pd.read_html(html_content)[0]
                    _df2.sort_values(by=['市场', '证券代码', '证券简称'], ascending=[True, True, True])
                    ##############
                    # 读取内容与上一次完全相同，则休息一下再重新读取内容
                    counter = 0
                    while _df1.equals(_df2):
                        logger.info(f'申万宏源担保券采集速度过快，休息一下{counter}')
                        time.sleep(0.01)
                        html_content = str(driver.page_source)
                        _df2 = pd.read_html(html_content)[0]
                        _df2.sort_values(by=['市场', '证券代码', '证券简称'], ascending=[True, True, True])
                        if counter > 100:
                            driver.quit()
                            msg = f"申万宏源担保券采集速度过快,连续休息100次重试还是存在问题!"
                            logger.info(msg)
                            raise Exception(msg)
                        counter += 1
                    _df1 = _df2
                    self.tmp_df = pd.concat([self.tmp_df, _df1])
                    # 与上一次存在重复，否则需要整个重采
                    if self.tmp_df.duplicated(['市场', '证券代码', '证券简称']).sum() > 0:
                        dep_line = self.tmp_df[self.tmp_df.duplicated(['市场', '证券代码', '证券简称'], keep='last')]  # 查看删除重复的行
                        dep_list = dep_line.values.tolist()
                        msg = f'申万宏源担保券采集的重复数据为：{dep_list},共{len(dep_list)}条'
                        logger.info(msg)
                        raise Exception(msg)
                    self.collect_num = self.tmp_df.index.size
                    logger.info(f"申万宏源担保券第{current_page + 1}/{self.total_page}页，记录数{self.collect_num}/{self.total_num}条")
                current_page += 1
            self.data_text = self.tmp_df.to_csv(index=False)
        finally:
            driver.quit()


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
