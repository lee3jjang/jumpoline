import os
import time
import json
import sqlite3
import hashlib
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple
from selenium import webdriver
from selenium.common.exceptions import UnexpectedAlertPresentException, NoSuchElementException
from concurrent.futures import ProcessPoolExecutor, TimeoutError
from rich.console import Console
from rich.traceback import install
from rich.progress import track
from rich.logging import RichHandler
import itertools
import re

os.makedirs('data', exist_ok=True)
os.makedirs('log', exist_ok=True)
os.makedirs('result', exist_ok=True)

# conn = sqlite3.connect('data/jumpoline.db')

install()
report_file = open("log/report.log", "a", encoding='utf8')
console_file = Console(file=report_file)
logging.basicConfig(
    level='INFO',
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(show_path=False), RichHandler(console=console_file, show_path=False)],
)
logger = logging.getLogger('root')

options = webdriver.ChromeOptions()
# options.add_argument("--start-maximized")
options.add_argument("headless")
options.add_argument("disable-gpu")

#####################################################################################################

def _get_data(estate: webdriver.remote.webelement.WebElement) -> List[str]:

    data = {}
    data['id'] = re.compile(r"\d{6}").search(estate.find_element_by_css_selector('.s_left > .text > h4').get_attribute('onclick')).group()
    data['code'] = estate.find_element_by_css_selector('.s_left > .text > .cate_name > .nocode > strong').get_attribute('innerHTML')
    data['loc'] = estate.find_element_by_css_selector('.s_left > .text > .cate_name > .cate').get_attribute('innerHTML').split('<strong')[0].split(', ')[0]
    data['floor'] = estate.find_element_by_css_selector('.s_left > .text > .cate_name > .cate').get_attribute('innerHTML').split('<strong')[0].split(', ')[1]
    data['cate'] = estate.find_element_by_css_selector('.s_left > .text > .cate_name > .cate > strong').get_attribute('innerHTML')
    data['space'] = estate.find_element_by_css_selector('.s_left > .text > .cate_name > .space_label').get_attribute('textContent')
    try:
        data['franch'] = estate.find_element_by_css_selector('.franch_name').get_attribute('innerHTML')
    except NoSuchElementException:
        data['franch'] = ''
    data['date'] = estate.find_element_by_css_selector('.s_left > .text > .cate_name > .fl_r > .date > em').get_attribute('textContent')
    data['hits'] = estate.find_element_by_css_selector('.s_left > .text > .cate_name > .fl_r > .hits > em').get_attribute('textContent')
    data['title'] = estate.find_element_by_css_selector('.s_left > .text > h4').get_attribute('textContent')
    data['marks'] = ', '.join(list(map(lambda x: x.get_attribute('alt'), estate.find_elements_by_css_selector('.s_left > .text > h4 > span > img'))))
    data['subtitle'] = estate.find_element_by_css_selector('.s_left > .text > .bxsubtit').get_attribute('textContent')
    try:
        data['copy'] = estate.find_element_by_css_selector('.s_left > .text > .copy').get_attribute('textContent')
    except NoSuchElementException:
        data['copy'] = ''
    data['deposit'] = estate.find_element_by_css_selector('.s_left > .text > .price > span > strong').get_attribute('textContent')
    data['mthfee'] = estate.find_element_by_css_selector('.s_left > .text > .price > .mthfee > strong').get_attribute('textContent')
    data['prem'] = estate.find_element_by_css_selector('.s_left > .text > .price > .premium > strong').get_attribute('textContent').replace('(협상가능)', '')
    try:
        data['nego_ok'] = re.sub('[(|)]', '', estate.find_element_by_css_selector('.s_left > .text > .price > .premium > strong > .nego_ok').get_attribute('textContent'))
    except NoSuchElementException:
        data['nego_ok'] = ''
    data['regist'] = {'regist4self': '직거래', 'regist4rule': '중개거래'}.get(estate.find_element_by_css_selector('.s_left > .text > .bottom > p').get_attribute('class'), '#')
        
    return data


def _get_data_by_category(category):

    # access start page of a category
    driver = webdriver.Chrome(executable_path='chromedriver', options=options)
    url = 'https://www.jumpoline.com/_jumpo/jumpoListMaster.asp'
    driver.get(url)
    driver.execute_script(category)
    time.sleep(5)

    # get data from all pages
    logger.info(f'Category {category} 수집시작')
    records = []
    page_num = int(re.sub('[(|)|끝]', '', driver.find_elements_by_css_selector('#dvPaging > .paging > .pageNum > a')[-1].get_attribute('textContent')))
    # for page in range(1):
    for page in range(page_num):

        # access unit page
        logger.info(f'Page {page+1} 수집시작')
        if page != 0:
            driver.execute_script(f'Worker.draw_mid_data("{1+page}")')
            time.sleep(5)
        
        # get data from unit page
        jplist_many = driver.find_elements_by_class_name('jplist')
        real_estates = [jplist.find_elements_by_tag_name('li') for jplist in jplist_many]
        for estate in itertools.chain(*real_estates):
            row = _get_data(estate)
            logger.info(f"ID: {row['id']}, Code: {row['code']} 수집 (Category: {category}, Page: {page+1:>2}/{page_num})")
            records.append(row)
            time.sleep(.005) # 속도조절
        logger.info(f'Page {page+1} 수집완료')
    logger.info(f'Category {category} 수집완료')

    # export data
    today = datetime.now().strftime('%Y%m%d')
    # with open(f'result/records_{category}_{today}.json', 'w', encoding='utf-8') as records_json:
    #     json.dump(records, records_json, indent=4, ensure_ascii=False)
    # logger.info(f'결과 저장 (경로: result/records_{category}_{today}.json)')
    records_df = pd.DataFrame.from_records(records)
    records_df.to_csv(f'result/{category.replace("/", "")}_{today}.csv', index=False, encoding='utf-8')
    logger.info(f'결과 저장 (경로: result/{category.replace("/", "")}_{today}.csv)')

    driver.close()  


if __name__ == '__main__':

    # get driver
    driver = webdriver.Chrome(executable_path='chromedriver', options=options)

    # access main page
    url = 'https://www.jumpoline.com/_jumpo/jumpoListMaster.asp'
    driver.get(url)
    logger.info(f'Main Page 접근')
    time.sleep(5)

    # get categories
    categories = []
    divisions = driver.find_elements_by_css_selector('#Z_return_change_div > div > ul')
    for div in divisions:
        suv_divisions = div.find_elements_by_css_selector('li.item_text')
        for sub_div in suv_divisions:
            categories.append(sub_div.find_element_by_tag_name('a').get_attribute('onclick').split(';')[0])
    logger.info(f'Category 목록 수집완료({len(categories)}개)')
    time.sleep(1)
    driver.close()

    # make process pool executor
    executor = ProcessPoolExecutor(max_workers=os.cpu_count())
    future_list = []
    for category in categories:
        future = executor.submit(_get_data_by_category, category)
        future_list.append(future)
    for idx, future in enumerate(future_list):
        if future.done():
            logger.info(f"result : {future.result()}")
            continue        
        try:
            result = future.result(timeout=60)
        except TimeoutError:
            logger.info(f"[{idx} worker] Timeout error")
        else:
            logger.info(f"result : {result}")
    executor.shutdown(wait=False)
    logger.info(f'전체 수집완료')
