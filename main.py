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
from selenium.common.exceptions import UnexpectedAlertPresentException
from concurrent.futures import ProcessPoolExecutor, TimeoutError
from rich.console import Console
from rich.traceback import install
# from rich.progress import track
from rich.logging import RichHandler

os.makedirs('data', exist_ok=True)
os.makedirs('log', exist_ok=True)
os.makedirs('result', exist_ok=True)

conn = sqlite3.connect('data/jumpoline.db')

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
# options.add_argument("headless")
options.add_argument("disable-gpu")

#####################################################################################################

if __name__ == '__main__':
    driver = webdriver.Chrome(executable_path='chromedriver', options=options)
    url = 'https://www.jumpoline.com/_jumpo/jumpoListMaster.asp'
    driver.get(url)
    logger.info("===========")