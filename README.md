# About this library
Japanese Stock Data download from Yahoo! Finance Japan and provides follow functions,
- Download historical split data.
- Download non-adjusted/adjusted historical price data.
- Download corporate profile data.
- Download accounting data(indelendent or consolidate).

Attention: Yahoo! Finance Japan oficially restricts scraping access, if you use this library carefully.


## How to install
```
pip install git+https://github.com/sawadyrr5/YJDataReader
```

## How to use
```
from YJDataReader.io.data import DataReader
```

### 1. Download split data.
```
DataReader(8411, data_source='yahoojp_split')
```

### 2. Download price data.
If `adjust` option is True, price data has adjusted.(default False)
```
DataReader(8411, data_source='yahoojp', start='2008-12-19', end='2009-01-17', adjust=False)
```

### 3. Download corporate profile data.
```
DataReader(8411, data_source='yahoojp_profile')
```

### 4.1 Download independent(non-consolidate) account data.
```
DataReader(8411, data_source='yahoojp_independent')
```

### 4.2 Download consolidate account data.
```
DataReader(8411, data_source='yahoojp_consolidate')
```

## Example
[example](example.ipynb)
