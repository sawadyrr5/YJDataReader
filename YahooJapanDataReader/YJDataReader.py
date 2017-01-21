#!/usr/local/bin python
# -*- coding: UTF-8 -*-
from pandas_datareader import data
from pandas_datareader.yahoo.daily import YahooDailyReader
from time import sleep
import urllib.request
import urllib.error
import lxml.html
import numpy as np
import pandas as pd

_SLEEP_TIME = 1.0
_MAX_RETRY_COUNT = 5


class YahooJPPriceReader(YahooDailyReader):
    _url_base = 'history/?code={code}.T&sy={sy}&sm={sm}&sd={sd}&ey={ey}&em={em}&ed={ed}&tm=d&p={p}'
    _xpath_base = '//*[@id="main"]/div[5]/table//tr//'
    _xpath = dict(
        Date=_xpath_base + 'td[1]',
        Open=_xpath_base + 'td[2]',
        High=_xpath_base + 'td[3]',
        Low=_xpath_base + 'td[4]',
        Close=_xpath_base + 'td[5]',
        Volume=_xpath_base + 'td[6]'
    )
    _column_order = ['Open', 'High', 'Low', 'Close', 'Volume']

    def __init__(self, adjust=False, **kwargs):
        super().__init__(**kwargs)
        self.adjust = adjust

    @property
    def url(self):
        return 'http://info.finance.yahoo.co.jp/'

    def read(self):
        # Use _DailyBaseReader's definition
        df = self._read_one_data(self.url, params=self._get_params(self.symbols))
        if not df.empty:
            if self.adjust:
                df = self._adjust_price(df)
            df = df.loc[:, self._column_order]
        return df

    def _get_params(self, symbol):
        params = dict(
            code=symbol,
            sy=self.start.year,
            sm=self.start.month,
            sd=self.start.day,
            ey=self.end.year,
            em=self.end.month,
            ed=self.end.day,
            p=1
        )
        return params

    def _read_one_data(self, url, params):
        results = []
        base = self.url + self._url_base

        while True:
            # retrying _MAX_RETRY_COUNT
            for _ in range(1, _MAX_RETRY_COUNT):
                try:
                    url = base.format(**params)
                    html = urllib.request.urlopen(url).read()
                    root = lxml.html.fromstring(html)
                    break
                except urllib.error.HTTPError:
                    sleep(10.0)
            else:
                raise Exception

            # make mask-array that row contains price data.
            row_masks = [p.text.replace(',', '').replace('.', '').isdigit() for p in root.xpath(self._xpath['Open'])]

            table = pd.DataFrame()
            for key, xpath in self._xpath.items():
                # apply mask-array to column "Date" and "Open"
                if key in ('Date', 'Open'):
                    table[key] = [val.text for val, row_mask in zip(root.xpath(xpath), row_masks) if row_mask]
                else:
                    table[key] = [val.text for val in root.xpath(xpath)]

                # set date or numeric format
                if key == 'Date':
                    table[key] = pd.DatetimeIndex(data=pd.to_datetime(table[key], format='%Y年%m月%d日'))
                else:
                    table[key] = pd.to_numeric(table[key].apply(lambda x: x.replace(',', '')))

                table['Code'] = params['code']

            # if first or last page is empty then break
            if params['p'] == 1 and table.empty:
                return pd.DataFrame()
            elif table.empty or (params['p'] > 1 and table.equals(results[-1])):
                break

            results.append(table)
            params['p'] += 1
            sleep(_SLEEP_TIME)

        result = pd.concat(results, ignore_index=True)

        result = result.set_index(['Code', 'Date'])
        result = result.sort_index()
        return result

    def _adjust_price(self, price):
        # calculate daily split ratio
        split = DataReader(self.symbols, 'yahoojp_split')
        split = split.reset_index()
        split['Split_Ratio'] = np.log(split['Split_Ratio'])

        dates = price.reset_index().loc[:, ['Code', 'Date']]
        split = pd.merge(dates, split, on='Code', how='outer', suffixes=('', '_y'))
        split = split[split['Date'] < split['Date_y']].groupby(['Code', 'Date']).sum()
        split['Split_Ratio'] = np.exp(split['Split_Ratio'])

        result = price.join(split)
        result['Split_Ratio'].fillna(1, inplace=True)

        # adjust ohlcv
        for col in result.columns:
            if col in ('Open', 'High', 'Low', 'Close'):
                result[col] = result[col] / result['Split_Ratio']
            elif col == 'Volume':
                result[col] = result[col] * result['Split_Ratio']

        result = result.drop(labels='Split_Ratio', axis=1)
        return result


class YahooJPSplitReader(YahooDailyReader):
    _url_base = 'stocks/chart/?code={code}.T&ct=z&t=ay'
    _xpath_base = '//*[@class="optionFi marB10"]/table[1]/tr[5]/td/ul//li/'
    _xpath = dict(
        Date=_xpath_base + 'span',
        Ratio=_xpath_base + 'strong'
    )

    @property
    def url(self):
        return 'http://stocks.finance.yahoo.co.jp/'

    def _get_params(self, symbol):
        params = dict(
            code=symbol
        )
        return params

    def _read_one_data(self, url, params):
        base = self.url + self._url_base
        url = base.format(**params)

        try:
            html = urllib.request.urlopen(url).read()
        except urllib.error.HTTPError:
            sleep(3.0)

        root = lxml.html.fromstring(html)

        result = pd.DataFrame()
        result['Date'] = pd.to_datetime([dt.text for dt in root.xpath(self._xpath['Date'])], format='（%y/%m/%d）')
        result['Split_Ratio'] = [float(ratio.text.replace('[', '').replace(']', '').split(':')[1]) for ratio in
                                 root.xpath(self._xpath['Ratio'])]
        result['Code'] = params['code']

        result = result.set_index(['Code', 'Date'])
        sleep(_SLEEP_TIME)
        return result


class _YahooJPCorporateReader(YahooDailyReader):
    @property
    def url(self):
        return 'http://profile.yahoo.co.jp/'

    def _get_params(self, symbol):
        params = dict(
            code=symbol
        )
        return params


class YahooJPProfileReader(_YahooJPCorporateReader):
    _url_base = 'fundamental/{code}'
    _xpath_base = '//*[@id="pro_body"]//div//div//div/table/tr[1]/td/table/'
    _xpath = dict(
        name='//*[@id="pro_body"]/center//div/h1/strong',
        specify=_xpath_base + 'tr[1]/td[2]',
        consolidated_business=_xpath_base + 'tr[2]/td[2]',
        hq_address=_xpath_base + 'tr[3]/td[2]',
        telephone=_xpath_base + 'tr[5]/td[2]',
        sector=_xpath_base + 'tr[6]/td[2]/a',
        company_name_en=_xpath_base + 'tr[7]/td[2]',
        ceo_name=_xpath_base + 'tr[8]/td[2]',
        established_date=_xpath_base + 'tr[9]/td[2]',
        exchanges=_xpath_base + 'tr[10]/td[2]',
        ipo_date=_xpath_base + 'tr[11]/td[2]',
        closing=_xpath_base + 'tr[12]/td[2]',
        unit_shares=_xpath_base + 'tr[13]/td[2]',
        employees_independent=_xpath_base + 'tr[14]/td[2]',
        employees_consolidates=_xpath_base + 'tr[14]/td[4]',
        average_age=_xpath_base + 'tr[15]/td[2]',
        average_income=_xpath_base + 'tr[15]/td[4]'
    )

    def _read_one_data(self, url, params):
        base = self.url + self._url_base
        url = base.format(**params)

        html = urllib.request.urlopen(url).read()
        root = lxml.html.fromstring(html)

        try:
            result = {key: root.xpath(xpath)[0].text for key, xpath in self._xpath.items()}
        except IndexError:
            raise StockIDError

        result['code'] = self.symbols
        result['company_name_jp'] = result['name'].split('【')[0].replace('(株)', '')
        result['hq_address'] = result['hq_address'].replace('  [', '')
        result['ceo_name'] = result['ceo_name'].replace('\n', '')

        if result['unit_shares'] in ('単元株制度なし'):
            result['unit_shares'] = 'NULL'
        else:
            result['unit_shares'] = self._multi_replace(result['unit_shares'], ['株', ','], '')

        result['employees_independent'] = self._multi_replace(result['employees_independent'], ['人', ','], '')
        result['employees_consolidates'] = self._multi_replace(result['employees_consolidates'], ['人', ','], '')

        result['average_age'] = self._multi_replace(result['average_age'], ['歳'], '')
        result['average_income'] = self._multi_replace(result['average_income'], [',', '千円'], '')

        sleep(_SLEEP_TIME)
        return result

    @staticmethod
    def _multi_replace(string, olds, new):
        for old in olds:
            string = string.replace(old, new)
        else:
            if string in ('-', '‐'):
                return 'NULL'
            else:
                return string


class YahooJPIndependentReader(_YahooJPCorporateReader):
    _url_base = 'independent/{code}'

    def _read_one_data(self, url, params):
        base = self.url + self._url_base
        url = base.format(**params)

        result = pd.read_html(url, header=0)
        result = result[4]
        sleep(_SLEEP_TIME)
        return result


class YahooJPConsolidateReader(_YahooJPCorporateReader):
    _url_base = 'consolidate/{code}'

    def _read_one_data(self, url, params):
        base = self.url + self._url_base
        url = base.format(**params)

        result = pd.read_html(url, header=0)
        result = result[4]
        sleep(_SLEEP_TIME)
        return result


class StockIDError(Exception):
    pass


def DataReader(symbols, data_source=None, start=None, end=None, **kwargs):
    if data_source == 'yahoojp':
        adjust = kwargs.pop('adjust', None)
        return YahooJPPriceReader(symbols=symbols, start=start, end=end, adjust=adjust, **kwargs).read()
    elif data_source == 'yahoojp_split':
        return YahooJPSplitReader(symbols=symbols, **kwargs).read()
    elif data_source == 'yahoojp_profile':
        return YahooJPProfileReader(symbols=symbols, **kwargs).read()
    elif data_source == 'yahoojp_independent':
        return YahooJPIndependentReader(symbols=symbols, **kwargs).read()
    elif data_source == 'yahoojp_consolidate':
        return YahooJPConsolidateReader(symbols=symbols, **kwargs).read()
    else:
        return data.DataReader(name=symbols, data_source=data_source, start=start, end=end, **kwargs)


DataReader.__doc__ = data.DataReader.__doc__
