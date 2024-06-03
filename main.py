from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from concurrent.futures import ThreadPoolExecutor, as_completed

import configparser
import sys
import datetime
import time

import pandas as pd
import pytz
import uuid

from dbOperator import dbOperator
from extractConsensus import extractConsensus
from extractList import extractList
from extractQuote import extractQuote
from extractShort import extractShort
from extractAnnc import extractAnnc
from extractFinance import extractFinance


def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--ignore-ssl-errors=true")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--no-sandbox")  # Required for running as root user
    chrome_options.add_argument("--disable-dev-shm-usage")  # Required for running as root user
    chrome_options.add_argument("--window-size=%s" % "1920,1080")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/98.0.4758.102 Safari/537.36")

    driver = webdriver.Chrome(options=chrome_options)
    return driver


def read_config(configFile):
    config = configparser.ConfigParser()
    config.read(configFile)

    return {
        'config_dir': config.get('Database', 'config_dir'),
        'user': config.get('Database', 'user'),
        'password': config.get('Database', 'password'),
        'dsn': config.get('Database', 'dsn'),
        'wallet_location': config.get('Database', 'wallet_location'),
        'wallet_password': config.get('Database', 'wallet_password'),
        'list_link': config.get('LoadList', 'list_link'),
        'list_table': config.get('LoadList', 'list_table'),
        'list_column': config.get('LoadList', 'list_column'),
        'short_table': config.get('LoadList', 'short_table'),
        'short_column': config.get('LoadList', 'short_column'),
        'short_url': config.get('LoadList', 'short_url'),
        'annc_prev_url': config.get('LoadList', 'annc_prev_url'),
        'annc_curr_url': config.get('LoadList', 'annc_curr_url'),
        'annc_table': config.get('LoadList', 'annc_table'),
        'annc_column': config.get('LoadList', 'annc_column'),
        'consensus_url': config.get('LoadList', 'consensus_url'),
        'consensus_table': config.get('LoadList', 'consensus_table'),
        'consensus_column': config.get('LoadList', 'consensus_column'),
        'finance_list_query': config.get('LoadList', 'finance_list_query'),

        'fi_prefix_url': config.get('LoadList', 'fi_prefix_url'),
        'fi_postfix_url': config.get('LoadList', 'fi_postfix_url'),
        'finance_table': config.get('LoadList', 'finance_table'),
        'finance_column': config.get('LoadList', 'finance_column'),
        'quote_list_query': config.get('LoadList', 'quote_list_query'),
        'ticker_table': config.get('LoadList', 'ticker_table'),
        'ticker_column': config.get('LoadList', 'ticker_column'),
        'quote_table': config.get('LoadList', 'quote_table'),
        'quote_column': config.get('LoadList', 'quote_column'),
        'quote_url': config.get('LoadList', 'quote_url')
    }


def main(configFile, param):

    sydney_timezone = pytz.timezone('Australia/Sydney')
    random_uuid = str(uuid.uuid4())
    start_time = str(datetime.datetime.now(sydney_timezone))
    print(f"system uuid generated as {random_uuid}")
    print(f"program started at {start_time}")

    full_config = read_config(configFile)
    do = dbOperator(full_config)
    el = extractList()
    es = extractShort()
    ec = extractConsensus()
    ea = extractAnnc()
    ef = extractFinance()
    eq = extractQuote()
    if param == 'list':
        try:
            driver = create_driver()
            driver.get(full_config['list_link'])
            df = el.get_asx_list(driver, random_uuid)
            do.write_data_to_db(df, full_config['list_table'], full_config['list_column'])
            driver.quit()
            end_time = str(datetime.datetime.now(sydney_timezone))
            print(f"program ended at {end_time}")
            sys_log = pd.DataFrame(
                {'UUID': [random_uuid], 'JOB_NAME': ['LIST'], 'start_time': [start_time], 'end_time': [end_time]})
            do.write_data_to_db(sys_log, 'EQUITY.SYS_BATCH_LOG', 'JOB_NAME, START_TIME, END_TIME')
        except Exception as e:
            print(f"Extract LIST failed with error: {e}")
            print(f"program ended at {end_time}")

    elif param == 'short':
        try:
            df = es.get_short_list(full_config['short_url'], random_uuid)
            do.write_data_to_db(df, full_config['short_table'], full_config['short_column'])
            end_time = str(datetime.datetime.now(sydney_timezone))
            print(f"program ended at {end_time}")
            sys_log = pd.DataFrame(
                {'UUID': [random_uuid], 'JOB_NAME': ['SHORT'], 'start_time': [start_time], 'end_time': [end_time]})
            do.write_data_to_db(sys_log, 'EQUITY.SYS_BATCH_LOG', 'JOB_NAME, START_TIME, END_TIME')
        except Exception as e:
            print(f"Extract SHORT failed with error: {e}")
            print(f"program ended at {end_time}")

    elif param == 'consensus':
        try:
            driver = create_driver()
            driver.get(full_config['consensus_url'])
            df = ec.get_consensus_list(driver, random_uuid)
            do.write_data_to_db(df, full_config['consensus_table'], full_config['consensus_column'])
            driver.quit()
            end_time = str(datetime.datetime.now(sydney_timezone))
            print(f"program ended at {end_time}")
            sys_log = pd.DataFrame(
                {'UUID': [random_uuid], 'JOB_NAME': ['LIST'], 'start_time': [start_time], 'end_time': [end_time]})
            do.write_data_to_db(sys_log, 'EQUITY.SYS_BATCH_LOG', 'JOB_NAME, START_TIME, END_TIME')
        except Exception as e:
            print(f"Extract CONSENSUS failed with error: {e}")
            print(f"program ended at {end_time}")

    elif param == 'annc':
        try:
            driver = create_driver()
            driver.get(full_config['annc_prev_url'])
            prev_df = ea.get_annc_list(driver, random_uuid)
            do.write_data_to_db(prev_df, full_config['annc_table'], full_config['annc_column'])

            driver.get(full_config['annc_curr_url'])
            curr_df = ea.get_annc_list(driver, random_uuid)
            do.write_data_to_db(curr_df, full_config['annc_table'], full_config['annc_column'])

            driver.quit()
            end_time = str(datetime.datetime.now(sydney_timezone))
            print(f"program ended at {end_time}")
            sys_log = pd.DataFrame(
                {'UUID': [random_uuid], 'JOB_NAME': ['LIST'], 'start_time': [start_time], 'end_time': [end_time]})
            do.write_data_to_db(sys_log, 'EQUITY.SYS_BATCH_LOG', 'JOB_NAME, START_TIME, END_TIME')
        except Exception as e:
            print(f"Extract ANNC failed with error: {e}")
            print(f"program ended at {end_time}")
    elif param == 'finance':
        driver = create_driver()
        code_list = do.fetch_data_from_db(full_config['finance_list_query'])
        for index, row in code_list.iterrows():
            code = row['CODE']
            print(code)
            time.sleep(30)
            try:
                driver.get(full_config['fi_prefix_url'] + code + full_config['fi_postfix_url'])
                fidf = ef.get_financial_list(driver, code, random_uuid)
                do.write_data_to_db(fidf, full_config['finance_table'], full_config['finance_column'])
            except Exception as e:
                print(f"Test case {code} failed with error: {e}")
        driver.quit()
        end_time = str(datetime.datetime.now(sydney_timezone))
        sys_log = pd.DataFrame(
            {'UUID': [random_uuid], 'JOB_NAME': ['LIST'], 'start_time': [start_time], 'end_time': [end_time]})
        do.write_data_to_db(sys_log, 'EQUITY.SYS_BATCH_LOG', 'JOB_NAME, START_TIME, END_TIME')
    elif param == 'quote':
        code_list = pd.DataFrame(columns=['CODE'])
        try:
            code_list = do.fetch_data_from_db(full_config['quote_list_query'])
        except Exception as e:
            print(f"Quote failed with connect database to get the code_list on {e}")

        full_ticker = []
        full_quote = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(eq.get_quote, full_config['quote_url'], row['CODE'], random_uuid) for index, row in code_list.iterrows()]

            for future in as_completed(futures):
                ticker, quote = future.result()  # Get the result of the completed future
                full_ticker.extend(ticker)
                full_quote.extend(quote)

        df_tk = pd.DataFrame(full_ticker, columns=['UUID', 'CODE', 'TICK_PRICE', 'TICK_TIME'])
        dt_qt = pd.DataFrame(full_quote, columns=['UUID', 'CODE', 'TAG', 'INFO', 'UPDATE_TIME'])

        try:
            do.write_data_to_db(df_tk, full_config['ticker_table'], full_config['ticker_column'])
            do.write_data_to_db(dt_qt, full_config['quote_table'], full_config['quote_column'])

            end_time = str(datetime.datetime.now(sydney_timezone))
            print(f"program ended at {end_time}")
            sys_log = pd.DataFrame(
                {'UUID': [random_uuid], 'JOB_NAME': ['QUOTE'], 'start_time': [start_time], 'end_time': [end_time]})
            do.write_data_to_db(sys_log, 'EQUITY.SYS_BATCH_LOG', 'JOB_NAME, START_TIME, END_TIME')

        except Exception as e:
            print(f"Quote failed with write result to database on {e}")
            path = '/home/ubuntu/quote_backup/'
            ticker_name = f'ticker_{random_uuid}.csv'
            quote_name = f'quote_{random_uuid}.csv'
            df_tk.to_csv(path+ticker_name, index=False,header=True, encoding='utf-8')
            dt_qt.to_csv(path + quote_name, index=False, header=True, encoding='utf-8')
    else:
        print("failed to get the correct param")

    do.connection.close()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python main.py <config> <param> ")
    else:
        configFile = sys.argv[1]
        param = sys.argv[2]
        main(configFile, param)
