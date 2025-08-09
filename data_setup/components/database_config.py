"""
Improved Financial Markets Database Setup and Management
Updated to work with the new master assets table and Alpha Vantage OVERVIEW data
"""

import sqlite3
import pandas as pd
from datetime import datetime, date
import os
from typing import List, Dict, Any, Optional

class FinancialDatabase:
    def __init__(self, db_path: str = "database_and_schema/financial_markets.db"):
        """
        Initialize the database connection
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
        self.connection = None
    
    def connect(self):
        """Create connection to the database"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints
            print(f"Connected to database: {self.db_path}")
            return self.connection
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return None
    
    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")
    
    def create_database(self, schema_file: str = 'database_and_schema/schema.sql'):
        """
        Create the database tables using the SQL schema
        
        Args:
            schema_file (str): Path to SQL schema file (optional)
        """
        if not self.connection:
            self.connect()
        
        if schema_file and os.path.exists(schema_file):
            with open(schema_file, 'r') as file:
                schema_sql = file.read()
            try:
                self.connection.executescript(schema_sql)
                self.connection.commit()
                print(f"Database schema created successfully from {schema_file}")
            except sqlite3.Error as e:
                print(f"Error creating database schema: {e}")
        else:
            print("Creating database with embedded schema...")
            print("Run the SQL schema artifact to create tables first")
    
    def insert_asset(self, asset_data: Dict[str, Any]):
        """
        Insert or update a single asset with Alpha Vantage OVERVIEW data
        
        Args:
            asset_data (Dict): Dictionary containing asset information from Alpha Vantage OVERVIEW
        """
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        

        try:
            # Helper function to clean and convert values
            def clean_value(value, data_type='str'):
                if value in ['None', '-', 'N/A', '', None]:
                    return None
                if data_type == 'float':
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return None
                elif data_type == 'int':
                    try:
                        return int(float(value))
                    except (ValueError, TypeError):
                        return None
                elif data_type == 'date':
                    if value and value != 'None':
                        try:
                            return datetime.strptime(value, '%Y-%m-%d').date()
                        except ValueError:
                            return None
                    return None
                return str(value) if value else None
            
            insert_sql = """
                INSERT OR REPLACE INTO assets 
                (symbol, name, description, cik, exchange, currency, country, sector, industry, asset_type,
                market_capitalization, ebitda, pe_ratio, peg_ratio, book_value, dividend_per_share, 
                dividend_yield, eps, revenue_per_share_ttm, profit_margin, operating_margin_ttm,
                return_on_assets_ttm, return_on_equity_ttm, revenue_ttm, gross_profit_ttm, 
                diluted_eps_ttm, quarterly_earnings_growth_yoy, quarterly_revenue_growth_yoy,
                analyst_target_price, trailing_pe, forward_pe, price_to_sales_ratio_ttm,
                price_to_book_ratio, ev_to_revenue, ev_to_ebitda, beta, week_52_high, week_52_low,
                day_50_moving_average, day_200_moving_average, shares_outstanding, dividend_date,
                ex_dividend_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
        
            values = (
                    asset_data.get('Symbol'),
                    asset_data.get('Name'),
                    asset_data.get('Description'),
                    asset_data.get('CIK'),
                    asset_data.get('Exchange'),
                    asset_data.get('Currency', 'USD'),
                    asset_data.get('Country'),
                    asset_data.get('Sector'),
                    asset_data.get('Industry'),
                    asset_data.get('AssetType', 'Stock'),
                    clean_value(asset_data.get('MarketCapitalization'), 'int'),
                    clean_value(asset_data.get('EBITDA'), 'int'),
                    clean_value(asset_data.get('PERatio'), 'float'),
                    clean_value(asset_data.get('PEGRatio'), 'float'),
                    clean_value(asset_data.get('BookValue'), 'float'),
                    clean_value(asset_data.get('DividendPerShare'), 'float'),
                    clean_value(asset_data.get('DividendYield'), 'float'),
                    clean_value(asset_data.get('EPS'), 'float'),
                    clean_value(asset_data.get('RevenuePerShareTTM'), 'float'),
                    clean_value(asset_data.get('ProfitMargin'), 'float'),
                    clean_value(asset_data.get('OperatingMarginTTM'), 'float'),
                    clean_value(asset_data.get('ReturnOnAssetsTTM'), 'float'),
                    clean_value(asset_data.get('ReturnOnEquityTTM'), 'float'),
                    clean_value(asset_data.get('RevenueTTM'), 'int'),
                    clean_value(asset_data.get('GrossProfitTTM'), 'int'),
                    clean_value(asset_data.get('DilutedEPSTTM'), 'float'),
                    clean_value(asset_data.get('QuarterlyEarningsGrowthYOY'), 'float'),
                    clean_value(asset_data.get('QuarterlyRevenueGrowthYOY'), 'float'),
                    clean_value(asset_data.get('AnalystTargetPrice'), 'float'),
                    clean_value(asset_data.get('TrailingPE'), 'float'),
                    clean_value(asset_data.get('ForwardPE'), 'float'),
                    clean_value(asset_data.get('PriceToSalesRatioTTM'), 'float'),
                    clean_value(asset_data.get('PriceToBookRatio'), 'float'),
                    clean_value(asset_data.get('EVToRevenue'), 'float'),
                    clean_value(asset_data.get('EVToEBITDA'), 'float'),
                    clean_value(asset_data.get('Beta'), 'float'),
                    clean_value(asset_data.get('52WeekHigh'), 'float'),
                    clean_value(asset_data.get('52WeekLow'), 'float'),
                    clean_value(asset_data.get('50DayMovingAverage'), 'float'),
                    clean_value(asset_data.get('200DayMovingAverage'), 'float'),
                    clean_value(asset_data.get('SharesOutstanding'), 'int'),
                    clean_value(asset_data.get('DividendDate'), 'date'),
                    clean_value(asset_data.get('ExDividendDate'), 'date')
                )
        
            print(f"Number of values: {len(values)}")
            cursor.execute(insert_sql, values)
            
            self.connection.commit()
            print(f"Inserted/updated asset: {asset_data.get('Symbol')}")
            
        except sqlite3.Error as e:
            print(f"Error inserting asset data for {asset_data.get('Symbol', 'Unknown')}: {e}")
    
    def insert_assets_batch(self, assets_list: List[Dict[str, Any]]):
        """
        Insert multiple assets in batch
        
        Args:
            assets_list (List[Dict]): List of asset dictionaries
        """
        for asset in assets_list:
            self.insert_asset(asset)
        print(f"Batch insert completed: {len(assets_list)} assets")
    
    def insert_daily_prices(self, price_data: pd.DataFrame, symbol: str = None):
        """
        Insert daily price data into daily_prices table
        
        Args:
            price_data (pd.DataFrame): DataFrame with price data
            symbol (str): Symbol if not in DataFrame
        """
        if not self.connection:
            self.connect()
        
        try:
            # Prepare the DataFrame
            df = price_data.copy()
            
            # Add symbol if not present
            if symbol and 'symbol' not in df.columns:
                df['symbol'] = symbol
            
            # Reset index if date is in index
            if df.index.name == 'Date' or 'date' in str(df.index.name).lower():
                df = df.reset_index()
                if 'Date' in df.columns:
                    df = df.rename(columns={'Date': 'date'})
            
            # Standardize column names
            column_mapping = {
                'Open': 'open_price',
                'High': 'high_price', 
                'Low': 'low_price',
                'Close': 'close_price',
                'Adj Close': 'adjusted_close',
                'Volume': 'volume'
            }
            df = df.rename(columns=column_mapping)
            
            # Ensure we have required columns
            required_cols = ['symbol', 'date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                print(f"Warning: Missing columns {missing_cols}")
                return
            
            # Insert data
            df.to_sql('daily_prices', self.connection, if_exists='append', 
                     index=False, method='ignore')
            
            print(f"Inserted {len(df)} price records for {symbol or 'multiple symbols'}")
            
        except Exception as e:
            print(f"Error inserting price data: {e}")
    
    def insert_economic_indicators(self, indicator_data: List[Dict[str, Any]]):
        """
        Insert economic indicator data
        
        Args:
            indicator_data (List[Dict]): List of economic indicator records
        """
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        insert_sql = """
        INSERT OR REPLACE INTO economic_indicators 
        (indicator_name, indicator_code, date, value, unit, frequency, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            for indicator in indicator_data:
                cursor.execute(insert_sql, (
                    indicator.get('indicator_name'),
                    indicator.get('indicator_code'),
                    indicator.get('date'),
                    indicator.get('value'),
                    indicator.get('unit'),
                    indicator.get('frequency'),
                    indicator.get('source')
                ))
            
            self.connection.commit()
            print(f"Inserted {len(indicator_data)} economic indicator records")
            
        except sqlite3.Error as e:
            print(f"Error inserting economic indicators: {e}")
    
    def insert_market_indices(self, index_data: List[Dict[str, Any]]):
        """
        Insert market indices data
        
        Args:
            index_data (List[Dict]): List of market index records
        """
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        insert_sql = """
        INSERT OR REPLACE INTO market_indices 
        (symbol, date, index_value, daily_return, volume, total_market_cap, 
         pe_ratio, dividend_yield, price_to_book, constituent_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            for index_record in index_data:
                cursor.execute(insert_sql, (
                    index_record.get('symbol'),
                    index_record.get('date'),
                    index_record.get('index_value'),
                    index_record.get('daily_return'),
                    index_record.get('volume'),
                    index_record.get('total_market_cap'),
                    index_record.get('pe_ratio'),
                    index_record.get('dividend_yield'),
                    index_record.get('price_to_book'),
                    index_record.get('constituent_count')
                ))
            
            self.connection.commit()
            print(f"Inserted {len(index_data)} market index records")
            
        except sqlite3.Error as e:
            print(f"Error inserting market indices data: {e}")
    
    def insert_volatility_data(self, volatility_data: List[Dict[str, Any]]):
        """
        Insert volatility data
        
        Args:
            volatility_data (List[Dict]): List of volatility records
        """
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        insert_sql = """
        INSERT OR REPLACE INTO volatility_data 
        (underlying_symbol, volatility_type, date, volatility_value, volatility_period)
        VALUES (?, ?, ?, ?, ?)
        """
        
        try:
            for vol_record in volatility_data:
                cursor.execute(insert_sql, (
                    vol_record.get('underlying_symbol'),
                    vol_record.get('volatility_type'),
                    vol_record.get('date'),
                    vol_record.get('volatility_value'),
                    vol_record.get('volatility_period')
                ))
            
            self.connection.commit()
            print(f"Inserted {len(volatility_data)} volatility records")
            
        except sqlite3.Error as e:
            print(f"Error inserting volatility data: {e}")
    
    def get_asset_symbols(self, asset_type: str = None) -> List[str]:
        """Get asset symbols, optionally filtered by type"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        if asset_type:
            cursor.execute("SELECT symbol FROM assets WHERE is_active = TRUE AND asset_type = ?", (asset_type,))
        else:
            cursor.execute("SELECT symbol FROM assets WHERE is_active = TRUE")
        
        return [row[0] for row in cursor.fetchall()]
    
    def get_latest_price_date(self, symbol: str) -> Optional[str]:
        """Get the latest date for a specific symbol in daily_prices"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute("SELECT MAX(date) FROM daily_prices WHERE symbol = ?", (symbol,))
        result = cursor.fetchone()
        return result[0] if result[0] else None
    
    def get_asset_overview(self, symbol: str) -> Dict[str, Any]:
        """Get complete asset overview from the assets table"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM assets WHERE symbol = ?", (symbol,))
        result = cursor.fetchone()
        
        if result:
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, result))
        return {}
    
    def execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame
        
        Args:
            query (str): SQL query to execute
            params (tuple): Parameters for the query
        
        Returns:
            pd.DataFrame: Query results
        """
        if not self.connection:
            self.connect()
        
        try:
            if params:
                return pd.read_sql_query(query, self.connection, params=params)
            else:
                return pd.read_sql_query(query, self.connection)
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()
    
    def get_data_summary(self):
        """Print a comprehensive summary of data in the database"""
        if not self.connection:
            self.connect()
        
        print("\n=== FINANCIAL MARKETS DATABASE SUMMARY ===")
        
        # Count records in each table
        tables = ['assets', 'daily_prices', 'economic_indicators', 
                 'sector_performance', 'market_indices', 'volatility_data']
        for table in tables:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count:,} records")
        
        # Asset breakdown by type
        print("\n=== ASSET BREAKDOWN ===")
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT asset_type, COUNT(*) as count, 
                   SUM(market_capitalization) as total_market_cap
            FROM assets 
            WHERE is_active = TRUE 
            GROUP BY asset_type 
            ORDER BY count DESC
        """)
        
        for row in cursor.fetchall():
            asset_type, count, total_cap = row
            cap_str = f"${total_cap/1e12:.2f}T" if total_cap else "N/A"
            print(f"  {asset_type}: {count} assets, Total Market Cap: {cap_str}")
        
        # Sector breakdown
        print("\n=== SECTOR BREAKDOWN ===")
        cursor.execute("""
            SELECT sector, COUNT(*) as count, AVG(pe_ratio) as avg_pe
            FROM assets 
            WHERE is_active = TRUE AND sector IS NOT NULL
            GROUP BY sector 
            ORDER BY count DESC 
            LIMIT 10
        """)
        
        for row in cursor.fetchall():
            sector, count, avg_pe = row
            pe_str = f"{avg_pe:.1f}" if avg_pe else "N/A"
            print(f"  {sector}: {count} assets, Avg P/E: {pe_str}")
        
        # Date range for price data
        cursor.execute("SELECT MIN(date), MAX(date), COUNT(DISTINCT symbol) FROM daily_prices")
        date_range = cursor.fetchone()
        if date_range[0]:
            print(f"\nPrice data: {date_range[0]} to {date_range[1]} ({date_range[2]} symbols)")

def setup_database():
    """Main function to set up the database"""
    print("Setting up Improved Financial Markets Database...")
    print()

    # Initialize database
    db = FinancialDatabase(db_path="database_and_schema/financial_markets.db")
    db.connect()
    db.create_database(schema_file='database_and_schema/schema.sql')
    
    print("\nDatabase created and connection established.")
    print("\n=== NEXT STEPS ===")
    print("1. Use db.get_data_summary() to check your sample data")
    print("2. Use insert_asset() to add Alpha Vantage OVERVIEW data")
    print("3. Use insert_daily_prices() to add historical price data")
    print("4. Use other functions to insert other types of data")
    
    return db







### Testing Purpose 
'''
if __name__ == "__main__":
    # Example usage
    db = setup_database()
    
    # Example: Insert sample asset with Alpha Vantage-like data
    sample_asset = {
        'Symbol': 'AAPL',
        'Name': 'Apple Inc.',
        'Description': 'Apple Inc. designs, manufactures, and markets smartphones...',
        'Exchange': 'NASDAQ',
        'Sector': 'Technology',
        'Industry': 'Consumer Electronics',
        'AssetType': 'Stock',
        'MarketCapitalization': '3000000000000',
        'PERatio': '28.5',
        'DividendYield': '0.0045',
        'Beta': '1.24',
        'EPS': '6.16',
        '52WeekHigh': '199.62',
        '52WeekLow': '164.08'
    }
    
    db.insert_asset(sample_asset)
    db.get_data_summary()
    db.close()
'''