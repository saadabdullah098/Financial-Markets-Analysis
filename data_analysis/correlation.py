import riskfolio as rp 
import pyfolio as pf
import pandas as pd
import numpy as np
import yfinance as yf
import seaborn as sns

assets = [
    'CRWD',
    'PANW',
    'AAPL',
    'MSFT',
    'GOOG',
    'TSLA',
    'DIS',
    'AXP',
    'GLD',
    '^GSPC',
    'QBTS',
    'HIMS',
    'SOUN',
    'AXON',
    'TSM',
    'AMD',
    'NVDA',
    'RR',
    'UBER'
]

data = yf.download(assets, start='2018-01-01', end = '2025-08-07')
data = data.loc[:, "Close"]


returns = data.pct_change().dropna()
returns 
returns.median().sort_values(ascending=False).to_frame(name='median_return')

rp.plot_clusters(
    returns=returns,
    codependence='pearson',
    linkage='ward',
    k=None,
    max_k=10,
    leaf_order=True,
    dendrogram=True,
    ax=None
)

