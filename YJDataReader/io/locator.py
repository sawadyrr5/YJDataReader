#!/usr/local/bin python
# -*- coding: UTF-8 -*-


class BaseLocator(object):
    url = None
    url_base = None
    xpath = None
    xpath_base = None

    @property
    def url_base(self):
        return self.url_base

    @property
    def url(self):
        return self.url

    @property
    def xpath(self):
        return self.xpath


class SplitLocator(BaseLocator):
    url_base = "stocks/chart/?code={code}.T&ct=z&t=ay"
    url = "http://stocks.finance.yahoo.co.jp/"
    xpath_base = "//*[@class=\"optionFi marB10\"]/table[1]/tr[5]/td/ul//li/"
    xpath = {
        "Date": xpath_base + "span",
        "Ratio": xpath_base + "strong"
    }


class PriceLocator(BaseLocator):
    url_base = "history/?code={code}.T&sy={sy}&sm={sm}&sd={sd}&ey={ey}&em={em}&ed={ed}&tm=d&p={p}"
    url = "http://info.finance.yahoo.co.jp/"
    xpath_base = "//*[@id=\"main\"]/div[@class=\"padT12 marB10 clearFix\"]/table//tr//"
    xpath = {
        "Date": xpath_base + "td[1]",
        "Close": xpath_base + "td[5]",
        "Volume": xpath_base + "td[6]",
        "High": xpath_base + "td[3]",
        "Open": xpath_base + "td[2]",
        "Low": xpath_base + "td[4]"
    }

    @property
    def column_order(self):
        return ("Open", "High", "Low", "Close", "Volume")


class CorporateLocator(BaseLocator):
    url_base = "fundamental/{code}"
    url = "http://profile.yahoo.co.jp/"
    xpath_base = "//*[@id=\"pro_body\"]//div//div//div/table/tr[1]/td/table/"
    xpath = {
        "hq_address": xpath_base + "tr[3]/td[2]",
        "established_date": xpath_base + "tr[9]/td[2]",
        "average_age": xpath_base + "tr[15]/td[2]",
        "unit_shares": xpath_base + "tr[13]/td[2]",
        "exchanges": xpath_base + "tr[10]/td[2]",
        "telephone": xpath_base + "tr[5]/td[2]",
        "name": "//*[@id=\"pro_body\"]/center//div/h1/strong",
        "company_name_en": xpath_base + "tr[7]/td[2]",
        "average_income": xpath_base + "tr[15]/td[4]",
        "specify": xpath_base + "tr[1]/td[2]",
        "sector": xpath_base + "tr[6]/td[2]/a",
        "ipo_date": xpath_base + "tr[11]/td[2]",
        "ceo_name": xpath_base + "tr[8]/td[2]",
        "employees_consolidates": xpath_base + "tr[14]/td[4]",
        "employees_independent": xpath_base + "tr[14]/td[2]",
        "closing": xpath_base + "tr[12]/td[2]",
        "consolidated_business": xpath_base + "tr[2]/td[2]"
    }


class IndependentLocator(CorporateLocator):
    url_base = "independent/{code}"


class ConsolidateLocator(CorporateLocator):
    url_base = "consolidate/{code}"
