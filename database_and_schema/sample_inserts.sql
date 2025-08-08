-- Insert sample assets (major indices and sector ETFs)
INSERT INTO assets (symbol, name, sector, asset_type, exchange, market_capitalization, pe_ratio) VALUES
-- Major Market Indices/ETFs
('SPY', 'SPDR S&P 500 ETF Trust', 'Broad Market', 'ETF', 'NYSE Arca', 400000000000, 21.5),
('QQQ', 'Invesco QQQ Trust', 'Technology', 'ETF', 'NASDAQ', 180000000000, 25.8),
('IWM', 'iShares Russell 2000 ETF', 'Small Cap', 'ETF', 'NYSE Arca', 45000000000, 18.3),
('DIA', 'SPDR Dow Jones Industrial Average ETF', 'Broad Market', 'ETF', 'NYSE Arca', 25000000000, 19.2),

-- Sector ETFs
('XLF', 'Financial Select Sector SPDR Fund', 'Financial Services', 'ETF', 'NYSE Arca', 35000000000, 12.8),
('XLK', 'Technology Select Sector SPDR Fund', 'Technology', 'ETF', 'NYSE Arca', 55000000000, 26.4),
('XLE', 'Energy Select Sector SPDR Fund', 'Energy', 'ETF', 'NYSE Arca', 12000000000, 14.2),
('XLV', 'Health Care Select Sector SPDR Fund', 'Healthcare', 'ETF', 'NYSE Arca', 32000000000, 16.8),
('XLI', 'Industrial Select Sector SPDR Fund', 'Industrials', 'ETF', 'NYSE Arca', 18000000000, 20.1),
('XLP', 'Consumer Staples Select Sector SPDR Fund', 'Consumer Staples', 'ETF', 'NYSE Arca', 15000000000, 22.5),
('XLY', 'Consumer Discretionary Select Sector SPDR Fund', 'Consumer Discretionary', 'ETF', 'NYSE Arca', 22000000000, 28.7),
('XLRE', 'Real Estate Select Sector SPDR Fund', 'Real Estate', 'ETF', 'NYSE Arca', 8000000000, 25.3),
('XLU', 'Utilities Select Sector SPDR Fund', 'Utilities', 'ETF', 'NYSE Arca', 14000000000, 18.9),

-- Volatility Index
('VIX', 'CBOE Volatility Index', 'Volatility', 'Index', 'CBOE', NULL, NULL),

-- Major Individual Stocks
('AAPL', 'Apple Inc.', 'Technology', 'Stock', 'NASDAQ', 3000000000000, 28.5),
('MSFT', 'Microsoft Corporation', 'Technology', 'Stock', 'NASDAQ', 2800000000000, 32.1),
('GOOGL', 'Alphabet Inc.', 'Technology', 'Stock', 'NASDAQ', 1800000000000, 24.2),
('AMZN', 'Amazon.com Inc.', 'Consumer Discretionary', 'Stock', 'NASDAQ', 1600000000000, 45.8),
('TSLA', 'Tesla Inc.', 'Consumer Discretionary', 'Stock', 'NASDAQ', 800000000000, 58.3);

-- Insert sample economic indicators
INSERT INTO economic_indicators (indicator_name, indicator_code, date, value, unit, frequency, source) VALUES
('Federal Funds Rate', 'FEDFUNDS', '2024-01-01', 5.25, 'Percent', 'Monthly', 'FRED'),
('10-Year Treasury Constant Maturity Rate', 'GS10', '2024-01-01', 4.15, 'Percent', 'Daily', 'FRED'),
('Unemployment Rate', 'UNRATE', '2024-01-01', 3.8, 'Percent', 'Monthly', 'FRED'),
('Consumer Price Index for All Urban Consumers', 'CPIAUCSL', '2024-01-01', 307.5, 'Index 1982-84=100', 'Monthly', 'FRED'),
('Gross Domestic Product', 'GDP', '2023-10-01', 27000000, 'Billions of Dollars', 'Quarterly', 'FRED'),
('Consumer Confidence Index', 'UMCSENT', '2024-01-01', 78.8, 'Index 1966:Q1=100', 'Monthly', 'University of Michigan');
