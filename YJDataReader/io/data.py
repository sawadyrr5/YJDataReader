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
import pandas.compat as compat
from pandas import DataFrame
from YJDataReader.io.locator import SplitLocator, PriceLocator, CorporateLocator, IndependentLocator, ConsolidateLocator

_SLEEP_TIME = 0.5
_MAX_RETRY_COUNT = 3


class YJDailyReader(YahooDailyReader):
    def _get_crumb(self, retries):
        """
        2018/4/6 depredated対策としてダミークラス追加
        :param retries:
        :return:
        """
        # Scrape a history page for a valid crumb ID:
        tu = "https://finance.yahoo.com/quote/{}/history".format(self.symbols)
        response = self._get_response(tu,
                                      params=self.params, headers=self.headers)
        out = str(self._sanitize_response(response))
        # Matches: {"crumb":"AlphaNumeric"}
        # rpat = '"CrumbStore":{"crumb":"([^"]+)"}'

        # crumb = re.findall(rpat, out)[0]
        crumb = out
        return crumb.encode('ascii').decode('unicode-escape')


class YJSplitReader(YJDailyReader):
    locator = SplitLocator()

    @property
    def url(self):
        return self.locator.url

    def _get_params(self, symbol):
        params = {
            'code': symbol
        }
        return params

    def _read_one_data(self, url, params):
        base = self.url + self.locator.url_base
        url = base.format(**params)

        try:
            html = urllib.request.urlopen(url).read()
        except urllib.error.HTTPError:
            sleep(_SLEEP_TIME)

        root = lxml.html.fromstring(html)

        result = pd.DataFrame()
        result['Date'] = pd.to_datetime([dt.text for dt in root.xpath(self.locator.xpath['Date'])], format='（%y/%m/%d）')
        result['Split_Ratio'] = [float(ratio.text.replace('[', '').replace(']', '').split(':')[1]) for ratio in
                                 root.xpath(self.locator.xpath['Ratio'])]
        result['Code'] = params['code']

        result = result.set_index(['Code', 'Date'])
        sleep(_SLEEP_TIME)
        return result


class YJPriceReader(YJDailyReader):
    locator = PriceLocator()

    def __init__(self, adjust=False, **kwargs):
        super().__init__(**kwargs)
        self.adjust = adjust

    @property
    def url(self):
        return self.locator.url

    def read(self):
        # Use _DailyBaseReader's definition
        df = self._read_one_data(self.url, params=self._get_params(self.symbols))
        if not df.empty:
            if self.adjust:
                df = self._adjust_price(df)
            df = df.loc[:, self.locator.column_order]
        return df

    def _get_params(self, symbol):
        params = {
            'code': symbol,
            'sy': self.start.year,
            'sm': self.start.month,
            'sd': self.start.day,
            'ey': self.end.year,
            'em': self.end.month,
            'ed': self.end.day,
            'p': 1
        }
        return params

    def _read_one_data(self, url, params):
        results = []
        base = self.url + self.locator.url_base

        while True:
            # retrying _MAX_RETRY_COUNT
            for _ in range(0, _MAX_RETRY_COUNT):
                try:
                    url = base.format(**params)
                    html = urllib.request.urlopen(url).read()
                    root = lxml.html.fromstring(html)
                    break
                except urllib.error.HTTPError:
                    sleep(_SLEEP_TIME)
            else:
                raise Exception

            # make mask-array that row contains price data.
            row_masks = [p.text.replace(',', '').replace('.', '').isdigit()
                         for p in root.xpath(self.locator.xpath['Open'])]

            table = pd.DataFrame()
            for key, xpath in self.locator.xpath.items():
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
        split = DataReader(self.symbols, 'yahoojp_split').reset_index()

        split['Split_Ratio'] = np.log(split['Split_Ratio'])

        dates = price.reset_index().loc[:, ['Code', 'Date']]

        # self merge split_ratio and calculate cumsum split_ratio
        split = pd.merge(dates, split, on='Code', how='outer', suffixes=('', '_y'))
        split = split[split['Date'] < split['Date_y']].groupby(['Code', 'Date']).sum()
        split['Split_Ratio'] = np.exp(split['Split_Ratio'])

        result = price.join(split)
        result['Split_Ratio'].fillna(1, inplace=True)

        # adjust price
        for col in result.columns:
            if col in self.locator.column_order:
                result[col] = result[col] / result['Split_Ratio']
            elif col == 'Volume':
                result[col] = result[col] * result['Split_Ratio']

        result.drop(labels='Split_Ratio', axis=1, inplace=True)
        return result


class _YJCorporateReader(YJDailyReader):
    locator = CorporateLocator()

    @property
    def url(self):
        return self.locator.url

    def _get_params(self, symbol):
        params = {
            'code': symbol
        }
        return params

    def read(self):
        """ read data """
        # If a single symbol, (e.g., '1306')
        if isinstance(self.symbols, (compat.string_types, int)):
            df = self._read_one_data(self.url,
                                     params=self._get_params(self.symbols))
        # Or multiple symbols, (e.g., ['1306', '7203', '8411'])
        elif isinstance(self.symbols, DataFrame):
            # TODO: support multiple symbols.
            df = self._read_one_data(self.url,
                                     params=self._get_params(self.symbols[0]))
        else:
            # TODO: support multiple symbols.
            df = self._read_one_data(self.url,
                                     params=self._get_params(self.symbols[0]))
        return df


class YJProfileReader(_YJCorporateReader):
    def _read_one_data(self, url, params):
        base = self.url + self.locator.url_base
        url = base.format(**params)

        html = urllib.request.urlopen(url).read()
        root = lxml.html.fromstring(html)

        try:
            pass
            result = {k: root.xpath(xpath)[0].text
                      for (k, xpath) in self.locator.xpath.items()}
        except IndexError:
            raise SymbolError

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


class YJIndependentReader(_YJCorporateReader):
    locator = IndependentLocator()

    def _read_one_data(self, url, params):
        base = self.url + self.locator.url_base
        url = base.format(**params)

        result = pd.read_html(url, header=0)
        result = result[4]

        # TODO: Fix column name "Unnamed: 0"

        sleep(_SLEEP_TIME)
        return result


class YJConsolidateReader(_YJCorporateReader):
    locator = ConsolidateLocator()

    def _read_one_data(self, url, params):
        base = self.url + self.locator.url_base
        url = base.format(**params)

        result = pd.read_html(url, header=0)
        result = result[4]

        # TODO: Fix column name "Unnamed: 0"

        sleep(_SLEEP_TIME)
        return result


class SymbolError(Exception):
    pass


def DataReader(symbols, data_source=None, start=None, end=None, **kwargs):
    if data_source == 'yahoojp':
        adjust = kwargs.pop('adjust', None)
        return YJPriceReader(symbols=symbols, start=start, end=end, adjust=adjust, **kwargs).read()
    elif data_source == 'yahoojp_split':
        return YJSplitReader(symbols=symbols, **kwargs).read()
    elif data_source == 'yahoojp_profile':
        return YJProfileReader(symbols=symbols, **kwargs).read()
    elif data_source == 'yahoojp_independent':
        return YJIndependentReader(symbols=symbols, **kwargs).read()
    elif data_source == 'yahoojp_consolidate':
        return YJConsolidateReader(symbols=symbols, **kwargs).read()
    else:
        return data.DataReader(name=symbols, data_source=data_source, start=start, end=end, **kwargs)


DataReader.__doc__ = data.DataReader.__doc__

if __name__ == '__main__':
    split = DataReader(8411, data_source='yahoojp_split')
    print(
        split
    )

    df = DataReader(8411, data_source='yahoojp', start='2008-12-19', end='2009-01-17')
    print(
        df.head(5)
    )

    df = DataReader(8411, data_source='yahoojp', start='2008-12-19', end='2009-01-17', adjust=True)
    print(
        df.head(5)
    )

    # 3. Download corporate profile data.
    profile = DataReader(8411, data_source='yahoojp_profile')
    print(
        profile
    )

    # 4.1 Download independent(non-consolidate) account data.
    independent = DataReader(8411, data_source='yahoojp_independent')
    print(independent)

    # 4.2 Download consolidate account data.
    consolidate = DataReader(8411, data_source='yahoojp_consolidate')
    print(consolidate)
