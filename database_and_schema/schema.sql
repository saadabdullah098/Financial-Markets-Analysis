-- Improved Financial Markets Database Schema with Master Assets Table
-- Incorporating Alpha Vantage OVERVIEW data structure

-- Drop tables if they exist (for fresh start)
DROP TABLE IF EXISTS daily_prices;
DROP TABLE IF EXISTS economic_indicators;
DROP TABLE IF EXISTS sector_performance;
DROP TABLE IF EXISTS market_indices;
DROP TABLE IF EXISTS volatility_data;
DROP TABLE IF EXISTS assets;

-- Master Assets Table (incorporates Alpha Vantage OVERVIEW fields)
CREATE TABLE assets (
    symbol VARCHAR(10) PRIMARY KEY,
    
    -- Basic Information
    name VARCHAR(255),
    description TEXT,
    cik VARCHAR(20),
    exchange VARCHAR(50),
    currency VARCHAR(10) DEFAULT 'USD',
    country VARCHAR(50),
    
    -- Classification
    sector VARCHAR(100),
    industry VARCHAR(150),
    asset_type VARCHAR(20) CHECK (asset_type IN ('Stock', 'ETF', 'Index', 'Bond', 'Commodity')),
    
    -- Financial Metrics (from OVERVIEW)
    market_capitalization BIGINT,
    ebitda BIGINT,
    pe_ratio DECIMAL(8,2),
    peg_ratio DECIMAL(6,3),
    book_value DECIMAL(10,2),
    dividend_per_share DECIMAL(8,4),
    dividend_yield DECIMAL(6,4),
    eps DECIMAL(8,4),
    revenue_per_share_ttm DECIMAL(10,2),
    profit_margin DECIMAL(6,4),
    operating_margin_ttm DECIMAL(6,4),
    return_on_assets_ttm DECIMAL(6,4),
    return_on_equity_ttm DECIMAL(6,4),
    revenue_ttm BIGINT,
    gross_profit_ttm BIGINT,
    diluted_eps_ttm DECIMAL(8,4),
    quarterly_earnings_growth_yoy DECIMAL(6,4),
    quarterly_revenue_growth_yoy DECIMAL(6,4),
    analyst_target_price DECIMAL(10,2),
    trailing_pe DECIMAL(8,2),
    forward_pe DECIMAL(8,2),
    price_to_sales_ratio_ttm DECIMAL(6,3),
    price_to_book_ratio DECIMAL(6,3),
    ev_to_revenue DECIMAL(6,3),
    ev_to_ebitda DECIMAL(6,3),
    
    -- Trading Information
    beta DECIMAL(6,4),
    week_52_high DECIMAL(10,4),
    week_52_low DECIMAL(10,4),
    day_50_moving_average DECIMAL(10,4),
    day_200_moving_average DECIMAL(10,4),
    shares_outstanding BIGINT,
    dividend_date DATE,
    ex_dividend_date DATE,
    
    -- Metadata
    is_active BOOLEAN DEFAULT TRUE,
    last_updated DATETIME DEFAULT (datetime('now')),
    created_date DATE DEFAULT (date('now'))
);

-- Daily Price Data (References master assets table)
CREATE TABLE daily_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(12,4),
    high_price DECIMAL(12,4),
    low_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    adjusted_close DECIMAL(12,4),
    volume BIGINT,
    dividend_amount DECIMAL(8,4) DEFAULT 0,
    split_coefficient DECIMAL(8,4) DEFAULT 1,
    created_timestamp DATETIME DEFAULT (datetime('now')),
    FOREIGN KEY (symbol) REFERENCES assets(symbol) ON DELETE CASCADE,
    UNIQUE(symbol, date)
);

-- Economic Indicators (unchanged but improved)
CREATE TABLE economic_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_name VARCHAR(100) NOT NULL,
    indicator_code VARCHAR(20),
    date DATE NOT NULL,
    value DECIMAL(18,8),
    unit VARCHAR(50),
    frequency VARCHAR(20) CHECK (frequency IN ('Daily', 'Weekly', 'Monthly', 'Quarterly', 'Annual')),
    source VARCHAR(50),
    created_timestamp DATETIME DEFAULT (datetime('now')),
    UNIQUE(indicator_name, date)
);

-- Sector Performance (now references assets table)
CREATE TABLE sector_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    
    -- Performance Metrics
    total_return DECIMAL(10,6),
    market_cap_weighted_return DECIMAL(10,6),
    equal_weighted_return DECIMAL(10,6),
    volatility DECIMAL(8,4),
    sharpe_ratio DECIMAL(8,4),
    
    -- Sector Statistics
    number_of_assets INTEGER,
    total_market_cap BIGINT,
    avg_pe_ratio DECIMAL(8,2),
    avg_dividend_yield DECIMAL(6,4),
    
    created_timestamp DATETIME DEFAULT (datetime('now')),
    UNIQUE(sector, date)
);

-- Market Indices (now properly connected to assets)
CREATE TABLE market_indices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(10) NOT NULL,  -- Now references assets table
    date DATE NOT NULL,
    
    -- Index Values
    index_value DECIMAL(15,4),
    daily_return DECIMAL(10,6),
    volume BIGINT,
    
    -- Index Metrics
    total_market_cap BIGINT,
    pe_ratio DECIMAL(8,2),
    dividend_yield DECIMAL(6,4),
    price_to_book DECIMAL(6,3),
    
    -- Additional Index Data
    constituent_count INTEGER,
    top_10_weight_percentage DECIMAL(5,2),
    
    created_timestamp DATETIME DEFAULT (datetime('now')),
    FOREIGN KEY (symbol) REFERENCES assets(symbol) ON DELETE CASCADE,
    UNIQUE(symbol, date)
);

-- Volatility Data (now properly connected to assets)
CREATE TABLE volatility_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying_symbol VARCHAR(10) NOT NULL,
    volatility_type VARCHAR(30) NOT NULL, -- 'VIX', 'Realized', 'GARCH', 'Implied', etc.
    date DATE NOT NULL,
    
    -- Volatility Measurements
    volatility_value DECIMAL(8,4),
    volatility_period INTEGER, -- days (e.g., 30 for 30-day volatility)
    
    -- Additional Volatility Metrics
    volatility_skew DECIMAL(8,4),
    volatility_kurtosis DECIMAL(8,4),
    
    created_timestamp DATETIME DEFAULT (datetime('now')),
    FOREIGN KEY (underlying_symbol) REFERENCES assets(symbol) ON DELETE CASCADE,
    UNIQUE(underlying_symbol, volatility_type, date, volatility_period)
);

-- Create indexes for performance
CREATE INDEX idx_assets_sector ON assets(sector);
CREATE INDEX idx_assets_asset_type ON assets(asset_type);
CREATE INDEX idx_assets_exchange ON assets(exchange);
CREATE INDEX idx_daily_prices_symbol_date ON daily_prices(symbol, date);
CREATE INDEX idx_daily_prices_date ON daily_prices(date);
CREATE INDEX idx_economic_indicators_date ON economic_indicators(date);
CREATE INDEX idx_sector_performance_date ON sector_performance(date);
CREATE INDEX idx_sector_performance_sector ON sector_performance(sector);
CREATE INDEX idx_market_indices_symbol_date ON market_indices(symbol, date);
CREATE INDEX idx_volatility_data_symbol_date ON volatility_data(underlying_symbol, date);

-- Create enhanced views for analysis
-- View 1: Asset overview with latest metrics
CREATE VIEW asset_overview AS
SELECT 
    symbol,
    name,
    sector,
    asset_type,
    market_capitalization,
    pe_ratio,
    dividend_yield,
    beta,
    week_52_high,
    week_52_low,
    is_active
FROM assets
WHERE is_active = TRUE
ORDER BY market_capitalization DESC;

-- View 2: Latest prices with asset information
CREATE VIEW latest_asset_prices AS
SELECT 
    a.symbol,
    a.name,
    a.sector,
    a.asset_type,
    a.pe_ratio,
    dp.date,
    dp.close_price,
    dp.adjusted_close,
    dp.volume
FROM assets a
JOIN daily_prices dp ON a.symbol = dp.symbol
WHERE dp.date = (
    SELECT MAX(date) 
    FROM daily_prices dp2 
    WHERE dp2.symbol = a.symbol
);

-- View 3: Daily returns with asset context
CREATE VIEW daily_returns_enhanced AS
SELECT 
    dp.symbol,
    a.name,
    a.sector,
    a.asset_type,
    dp.date,
    dp.close_price,
    LAG(dp.close_price) OVER (PARTITION BY dp.symbol ORDER BY dp.date) as prev_close,
    CASE 
        WHEN LAG(dp.close_price) OVER (PARTITION BY dp.symbol ORDER BY dp.date) IS NOT NULL 
        THEN (dp.close_price - LAG(dp.close_price) OVER (PARTITION BY dp.symbol ORDER BY dp.date)) / 
             LAG(dp.close_price) OVER (PARTITION BY dp.symbol ORDER BY dp.date)
        ELSE NULL 
    END as daily_return
FROM daily_prices dp
JOIN assets a ON dp.symbol = a.symbol
ORDER BY dp.symbol, dp.date;

-- View 4: Sector analysis summary
CREATE VIEW sector_analysis AS
SELECT 
    a.sector,
    COUNT(*) as asset_count,
    AVG(a.pe_ratio) as avg_pe_ratio,
    AVG(a.dividend_yield) as avg_dividend_yield,
    AVG(a.beta) as avg_beta,
    SUM(a.market_capitalization) as total_market_cap,
    MIN(a.market_capitalization) as min_market_cap,
    MAX(a.market_capitalization) as max_market_cap
FROM assets a
WHERE a.is_active = TRUE 
  AND a.pe_ratio IS NOT NULL
GROUP BY a.sector
ORDER BY total_market_cap DESC;