# YahooJapanDataReader
Yahoo! Financeから日本株の価格データ等を取得するモジュールです. 分割を考慮します.
k-db.comから取得する場合に個別株の価格が分割考慮されていないのが嫌だという場合は役に立つかと思います.
その他, 銘柄情報や決算なども(ざっくりとした感じに)取得できます.

Yahoo! Finance公式にはスクレイピングはダメよ～ダメダメと言っているので, まぁ迷惑をかけないようにゴニョゴニョ.

## インストール
```
pip install YahooJapanDataReader
```

## 使い方
importしてください
```
from YahooJapanDataReader.io.data import DataReader
```

### 分割情報を調べる
DataFrameで返ってきますSplit_Ratioが分割比率です(1000なら旧1株->新1000株)
```
DataReader(8411, data_source='yahoojp_split')
```

### 株価を取得する(分割考慮しない)
DataFrameで返ってきます
```
DataReader(8411, data_source='yahoojp', start='2008-12-19', end='2009-01-17')
```

### 株価を取得する(分割考慮する)
DataFrameで返ってきます
```
DataReader(8411, data_source='yahoojp', start='2008-12-19', end='2009-01-17', adjust=True)
```

### 会社情報を取得する
dictで返ってきます
```
DataReader(8411, data_source='yahoojp_profile')
```

### 個別決算を取得する
あんまり気を使ってない感じのDataFrameで返ってきます
```
DataReader(8411, data_source='yahoojp_independent')
```

### 連結決算を取得する
あんまり気を使ってない感じのDataFrameで返ってきます
```
DataReader(8411, data_source='yahoojp_consolidate')
```

サンプルはこちら
[example](https://github.com/sawadyrr5/YahooJapanDataReader/blob/master/YahooJapanDataReader/example.ipynb)
