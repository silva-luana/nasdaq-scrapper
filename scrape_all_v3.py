import requests, sys
from xml.etree import ElementTree
import xmltodict
import pandas as pd
from datetime import datetime, timedelta
import time, calendar, json, sched
import sqlite3
from sqlite3 import Error
from multiprocessing import Pool
import os.path

class NetworkError(RuntimeError):
    pass

def retryer(max_retries=5, timeout=5):
    def wraps(func):
        request_exceptions = (
            requests.Timeout,
            requests.ConnectionError,
            requests.HTTPError
        )
        def inner(*args, **kwargs):
            for i in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                except request_exceptions:
                    print('Can\'t connect to the internet. Retrying..')
                    time.sleep(timeout)
                    continue
                else:
                    return result
            else:
                raise NetworkError
        return inner
    return wraps

class nasdaqScraper():

    s = sched.scheduler(time.time, time.sleep)

    def __init__(self):
        self.conn = self.connect_to_db('nasdaqPricesFrame.db')
        self.conn.execute("create table if not exists prices(time, symbol, high, low, open, close, volume)")
        self.data = []

    def get_nasdaq_codes(self):
        """
            Pega os códigos/nomes dos ativos que quer-se buscar os dados a partir do CSV passado
        """
        df = pd.read_csv('nasdaq100list.csv')
        return df['Symbol'].tolist()

    def connect_to_db(self, dbName):
        """
            Conecta ao banco de dados SQLite
        """
        try:
            conn = sqlite3.connect(dbName)
        except Error as e:
            return e
        return conn

    def request_data(self):
        """
            Faz o request para o site do Yahoo Finance com cada um dos 100 assets em um único Pool (mapeia o request nos 100 ativos em 100 threads)
        """
        assets = self.get_nasdaq_codes()
        #assets = ['AAPL', 'GOOG', 'AMZN']
        urls = []
        for asset in assets:
            time1 = calendar.timegm((datetime.now() - timedelta(minutes=1)).utctimetuple())
            time2 = calendar.timegm(datetime.now().utctimetuple())

            url = "https://query1.finance.yahoo.com/v8/finance/chart/" + asset + "?symbol=" + asset + "&period1=" + str(time1) + \
                "&period2=" + str(time2) + "&interval=1m&includePrePost=true&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com"

            urls.append(url)

        p = Pool(len(assets))
        return p.map(requests.get, urls)

    def convert_tmstp(self, tmstp):
        """
            Formata dos dados de timestamp para 'YYYY-MM-DD horas:minutos:segundos'
        """
        return datetime.fromtimestamp(float(tmstp)).strftime('%Y-%m-%d %H:%M:%S')

    def format_data(self, response):
        """
            Formata os dados, metadados e erros vindos do request (XML)
        """
        error = response['chart']['error']
        meta = response['chart']['result'][0]['meta']
        df = pd.DataFrame(columns=['time', 'symbol', 'high', 'low', 'open', 'close', 'volume'])
        if len(response['chart']['result'][0]['indicators']['quote'][0]) > 1:
            df.time = [self.convert_tmstp(t) for t in response['chart']['result'][0]['timestamp']]
            df.symbol = meta['symbol']
            df.high = response['chart']['result'][0]['indicators']['quote'][0]['high']
            df.low = response['chart']['result'][0]['indicators']['quote'][0]['low']
            df.volume = response['chart']['result'][0]['indicators']['quote'][0]['volume']
            df.close = response['chart']['result'][0]['indicators']['quote'][0]['close']
            df.open = response['chart']['result'][0]['indicators']['quote'][0]['open']
        df = df.set_index('time').dropna()
        return df, meta, error

    def save_data(self, sc):
        """
            Salva os dados no banco de dados
        """
        for request in self.request_data():
            response = json.loads(request.text)
            df, meta, error = self.format_data(response)
            if not error:
                if not df.empty:
                    try:
                        df.to_sql("prices", self.conn, if_exists="append")
                        print(df.symbol[0], ' saved at %s' % datetime.now())
                    except:
                        print('não foi possível adicionar ', df.symbol, ' em prices.')
                else:
                    nulls = [self.convert_tmstp(time.time()), meta['symbol'], None, None, None, None, None]
                    self.conn.execute("insert into prices(time, symbol, high, low, open, close, volume) values (?, ?, ?, ?, ?, ?, ?)", nulls)

    def run(self):
        self.s.enter(50, 10, self.save_data, (self.s,))
        self.s.run()

if __name__ == '__main__':

    while True:
        scraper = nasdaqScraper()
        scraper.run()