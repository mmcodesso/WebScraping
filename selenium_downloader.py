import csv
from pathos.pools import ProcessPool
from pathos.helpers import cpu_count
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException


target_url = 'http://performance.morningstar.com/fund/performance-return.action?t={}&region=usa&culture=en-US'
explicit_wait = 15


def download(ticker):

    url = target_url.format(ticker)
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')

    driver = webdriver.Chrome(chrome_options=options)

    try:
        driver.get(url)

        wait = WebDriverWait(driver, explicit_wait)
        wait.until(EC.presence_of_all_elements_located((By.ID, "fund1")))

        q11 = driver.find_element_by_xpath(
            xpath='//*[@id="div_quarterly_returns"]/table/tbody[2]/tr[2]/td[1]').get_property('innerHTML')
        q21 = driver.find_element_by_xpath(
            xpath='//*[@id="div_quarterly_returns"]/table/tbody[2]/tr[3]/td[1]').get_property('innerHTML')
        q31 = driver.find_element_by_xpath(
            xpath='//*[@id="div_quarterly_returns"]/table/tbody[2]/tr[4]/td[1]').get_property('innerHTML')
        q41 = driver.find_element_by_xpath(
            xpath='//*[@id="div_quarterly_returns"]/table/tbody[2]/tr[5]/td[1]').get_property('innerHTML')

    except:
        q11 = "N/A"
        q21 = "N/A"
        q31 = "N/A"
        q41 = "N/A"

    with open(output_file, 'a') as out:
        out.write(ticker + ',' + q11 + ',' + q21 +
                  ',' + q31 + ',' + q41 + '\n')

    driver.close()

    return print(ticker, ',', q11, ',', q21, ',', q31, ',', q41)


def iter_path_generator(ticker_file):
    with open(ticker_file, 'r') as fin:
        reader = csv.reader(fin, delimiter=',',
                            quotechar='\"', quoting=csv.QUOTE_ALL)
        for _, row in enumerate(reader, 1):
            ticker, _ = row
            yield ticker


if __name__ == "__main__":

    ticker_file = 'Ticker-Fund.csv'
    output_file = 'ticker_values.csv'
    # Processing
    ncpus = cpu_count() if cpu_count() <= 8 else 8
    pool = ProcessPool(ncpus)
    pool.map(download, iter_path_generator(ticker_file))
