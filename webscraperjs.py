import pandas as pd
import os
import time
import sqlite3
from time import sleep
from requests_html import HTMLSession
from pathos.pools import ProcessPool


class WebscraperJs:

    def __init__(self, input_file, output_file='results.sqlite',
                 chunk_size=200, n_cpu=8):
        self.input_file = input_file
        self.output_file = output_file
        self.chunk_size = chunk_size
        self.session = 0
        self.pool = ProcessPool(n_cpu)

    def create_database(self):
        """Create database"""
        with sqlite3.connect(self.output_file) as db:
            cursor = db.cursor()
            cursor.execute('''CREATE TABLE tickers\
            (ticker TEXT PRIMARY KEY, q1 TEXT, q2 TEXT, q3 TEXT, q4 TEXT)''')
            db.commit()

        return

    def download(self, ticker):
        """Download the information and save the results in output_file"""
        target_url = 'http://performance.morningstar.com/fund/performance-return.action?t={}&region=usa&culture=en-US'
        url = target_url.format(ticker)

        try:
            r = self.session.get(url)
            r.html.render(sleep=1.0)

            q1 = r.html.xpath(
                '//*[@id="div_quarterly_returns"]/table/tbody[2]/tr[2]/td[1]')[0].text
            q2 = r.html.xpath(
                '//*[@id="div_quarterly_returns"]/table/tbody[2]/tr[3]/td[1]')[0].text
            q3 = r.html.xpath(
                '//*[@id="div_quarterly_returns"]/table/tbody[2]/tr[4]/td[1]')[0].text
            q4 = r.html.xpath(
                '//*[@id="div_quarterly_returns"]/table/tbody[2]/tr[5]/td[1]')[0].text

        except:
            q1 = "N/A"
            q2 = "N/A"
            q3 = "N/A"
            q4 = "N/A"

        with sqlite3.connect(self.output_file) as db:
            cursor = db.cursor()
            cursor.execute('''INSERT INTO  tickers(ticker, q1, q2, q3, q4) \
                           VALUES(?, ?, ?, ?, ?)''', (ticker, q1, q2, q3, q4))
            db.commit()

        message = "Downloading ticker: {} - {},{},{},{}"

        return print(message.format(ticker, q1, q2, q3, q4))

    def poll_mapping(self, func1, func2):
        """Creates the pooling for multi treathing."""
        self.pool.map(func1, func2)
        self.pool.close()
        self.pool.restart()
        return

    def kill_process(self):
        """Kill the process Chromium that is freezed."""
        print('Killing process')
        command = "taskkill /f /t /im chrome.exe"
        os.system(command)
        sleep(1.0)
        return

    def run(self):
        """ This funcion run the process """
        start_time = time.time()
        chunks = pd.read_csv(self.input_file, chunksize=self.chunk_size,
                             iterator=True, header=None,
                             names=['ticker', 'description'])
        for chunk in chunks:
            batch_time = time.time()
            ticker = list(chunk['ticker'])
            self.session = HTMLSession()
            self.pool.map(self.download, ticker)
            self.session.close()
            self.pool.close()
            self.pool.restart()
            self.kill_process()
            print("batch time: ", str(round(time.time() - batch_time)),
                  " seconds.")

        str_time = str(round(time.time() - start_time))  # str with total time
        total_time = "Total time: " + str_time + " seconds."
        return print(total_time)


if __name__ == "__main__":

    input_file = 'Ticker-Fund.csv'
    output_file = 'results.sqlite'
    chunk_size = 500
    n_cpu = 8 # Set numbers of cores, half os the machines cores is ok

    tickers = WebscraperJs(input_file, output_file, chunk_size, n_cpu)
    tickers.create_database()
    tickers.run()
