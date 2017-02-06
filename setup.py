# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='YahooJapanDataReader',
    version='0.0.4',
    description='Read japanese stock price and corporate profile data from Yahoo! Finance Japan.',
    author='sawadyrr5',
    author_email='riskreturn5@gmail.com',
    url='https://github.com/sawadyrr5/YahooJapanDataReader',
    packages=find_packages(),
    install_requires=['pandas', 'pandas_datareader', 'lxml']
    )
