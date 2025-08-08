# Financial-Markets-Analysis

# Financial Markets Portfolio Project 
## Key Research Questions to Answer
#### Market Performance & Risk Analysis:
    - Which sectors/stocks show the highest volatility vs returns over the past 2-3 years?
    - How do different asset classes (stocks, bonds, commodities) correlate during market stress periods?
    - What's the risk-adjusted performance (Sharpe ratio) across different market cap segments?

#### Market Timing & Trends:
    - Do certain months/quarters consistently outperform others (seasonal effects)?
    - How do major economic events (Fed meetings, earnings seasons) impact market volatility?
    - What's the relationship between trading volume and price movements?

#### Sector & Stock Analysis:
    - Which sectors are most sensitive to interest rate changes or economic indicators?
    - How do individual stock performances compare to their sector benchmarks?
    - What's the impact of major news events on stock price movements?

## Data Sources & APIs
Primary: 
    - Alpha Vantage
Alternative Sources:
    - Yahoo Finance (yfinance Python library): Unlimited free access, great for stock/ETF data
    - FRED API: Federal Reserve economic data (interest rates, economic indicators)
    - Polygon.io: Real-time and historical market data (free tier available)
    - IEX Cloud: Financial data API with generous free tier

## Project Structure & Documentation
financial_markets_analysis/
├── data_collection/
│   ├── api_data_pull.py
│   └── data_validation.ipynb
├── database/
│   ├── create_tables.sql
│   ├── financial_markets.db
│   └── data_insertion.py
├── analysis/
│   ├── eda_analysis.ipynb
│   ├── statistical_analysis.ipynb
│   └── risk_analysis.ipynb
├── sql_queries/
│   └── tableau_queries.sql
├── tableau/
│   └── financial_dashboards.twbx
└── README.md