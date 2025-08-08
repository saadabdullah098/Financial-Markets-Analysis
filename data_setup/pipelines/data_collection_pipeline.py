"""
Financial Data Pipeline - Main Orchestrator
Combines data collection and database operations using both modules
"""

import logging
from datetime import datetime
from typing import List, Tuple, Dict, Any
import pandas as pd

# Import our modules
from data_setup.components.database_config import FinancialDatabase
from data_setup.components.data_collection_config import FinancialDataCollector

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FinancialDataPipeline:
    """
    Main orchestrator that combines data collection and database operations
    """
    
    def __init__(self, db_path: str = "FMA/database/financial_markets.db", 
                 alpha_vantage_key: str = None, 
                 fred_api_key: str = None):
        """
        Initialize the financial data pipeline
        
        Args:
            db_path (str): Path to SQLite database
            alpha_vantage_key (str): Alpha Vantage API key
            fred_api_key (str): FRED API key
        """
        self.db = FinancialDatabase(db_path)
        self.collector = FinancialDataCollector(alpha_vantage_key, fred_api_key)
        
        # Connect to database
        self.db.connect()
        
        logger.info("Financial Data Pipeline initialized")
    
    def setup_database(self, schema_file: str = 'FMA/database/schema.sql'):
        """
        Set up the database with schema
        
        Args:
            schema_file (str): Path to SQL schema file
        """
        logger.info("Setting up database schema...")
        self.db.create_database(schema_file)
        logger.info("Database setup complete")
    
    def collect_and_insert_asset(self, symbol: str, asset_type: str = "Stock") -> bool:
        """
        Collect comprehensive asset data and insert into database
        
        Args:
            symbol (str): Stock symbol
            asset_type (str): Type of asset (Stock, ETF, etc.)
            
        Returns:
            bool: Success status
        """
        logger.info(f"Processing asset: {symbol} ({asset_type})")
        success = False
        
        # Step 1: Collect Alpha Vantage overview data
        overview_data = self.collector.get_asset_overview_alpha_vantage(symbol)
        
        # Step 2: Collect yfinance backup data
        yf_info = self.collector.get_asset_info_yfinance(symbol)
        
        # Step 3: Prepare asset data for insertion
        if overview_data:
            # Use Alpha Vantage data (primary source)
            asset_data = self._map_alpha_vantage_to_asset(overview_data, asset_type)
        elif yf_info:
            # Fallback to yfinance data
            asset_data = self._map_yfinance_to_asset(symbol, yf_info, asset_type)
        else:
            logger.warning(f"No asset overview data found for {symbol}")
            asset_data = self._create_minimal_asset(symbol, asset_type)
        
        # Step 4: Insert asset data using database module
        try:
            self.db.insert_asset(asset_data)
            logger.info(f"✓ Inserted asset overview for {symbol}")
            success = True
        except Exception as e:
            logger.error(f"✗ Error inserting asset data for {symbol}: {e}")
        
        # Step 5: Collect and insert price data
        price_data = self.collector.get_price_data_yfinance(symbol, period="2y")
        if price_data is not None:
            try:
                self.db.insert_daily_prices(price_data, symbol)
                logger.info(f"✓ Inserted {len(price_data)} price records for {symbol}")
                success = True
            except Exception as e:
                logger.error(f"✗ Error inserting price data for {symbol}: {e}")
        
        return success
    
    def collect_and_insert_economic_indicators(self):
        """
        Collect and insert economic indicators from FRED
        """
        logger.info("Collecting economic indicators...")
        
        indicators = self.collector.get_fred_indicators()
        indicator_records = []
        
        for series_id, metadata in indicators.items():
            # Collect data
            fred_data = self.collector.get_fred_economic_data(series_id)
            
            if fred_data is not None:
                # Convert to format expected by database
                for _, row in fred_data.iterrows():
                    indicator_records.append({
                        'indicator_name': metadata['name'],
                        'indicator_code': series_id,
                        'date': row['date'].strftime('%Y-%m-%d'),
                        'value': float(row['value']) if pd.notnull(row['value']) else None,
                        'unit': metadata['unit'],
                        'frequency': metadata['frequency'],
                        'source': metadata['source']
                    })
        
        # Insert using database module
        if indicator_records:
            try:
                self.db.insert_economic_indicators(indicator_records)
                logger.info(f"✓ Inserted {len(indicator_records)} economic indicator records")
            except Exception as e:
                logger.error(f"✗ Error inserting economic indicators: {e}")
        else:
            logger.warning("No economic indicator data collected")
    
    def collect_and_insert_volatility_data(self, symbol: str):
        """
        Collect and insert volatility data for a symbol
        
        Args:
            symbol (str): Stock symbol
        """
        logger.info(f"Collecting volatility data for {symbol}...")
        
        # Get price data for volatility calculation
        price_data = self.collector.get_price_data_yfinance(symbol, period="1y")
        
        if price_data is not None:
            # Calculate realized volatility
            volatility_df = self.collector.calculate_realized_volatility(price_data, window=30)
            
            if not volatility_df.empty:
                # Convert to format expected by database
                volatility_records = []
                for _, row in volatility_df.iterrows():
                    volatility_records.append({
                        'underlying_symbol': row['symbol'],
                        'volatility_type': row['volatility_type'],
                        'date': row['date'].strftime('%Y-%m-%d'),
                        'volatility_value': float(row['volatility_value']) if pd.notnull(row['volatility_value']) else None,
                        'volatility_period': int(row['volatility_period'])
                    })
                
                # Insert using database module
                try:
                    self.db.insert_volatility_data(volatility_records)
                    logger.info(f"✓ Inserted {len(volatility_records)} volatility records for {symbol}")
                except Exception as e:
                    logger.error(f"✗ Error inserting volatility data for {symbol}: {e}")
    
    def update_sector_performance(self):
        """
        Calculate and update sector performance metrics
        """
        logger.info("Updating sector performance metrics...")
        
        # Get sectors from database
        sectors_query = "SELECT DISTINCT sector FROM assets WHERE sector IS NOT NULL AND sector != '' AND is_active = TRUE"
        sectors_df = self.db.execute_query(sectors_query)
        
        if sectors_df.empty:
            logger.warning("No sectors found in database")
            return
        
        sector_records = []
        today = datetime.now().strftime('%Y-%m-%d')
        
        for sector in sectors_df['sector'].unique():
            # Calculate sector metrics
            sector_query = """
                SELECT 
                    COUNT(*) as asset_count,
                    SUM(market_capitalization) as total_market_cap,
                    AVG(pe_ratio) as avg_pe_ratio,
                    AVG(dividend_yield) as avg_dividend_yield
                FROM assets 
                WHERE sector = ? AND is_active = TRUE
            """
            
            result = self.db.execute_query(sector_query, params=(sector,))
            
            if not result.empty:
                row = result.iloc[0]
                sector_records.append({
                    'sector': sector,
                    'date': today,
                    'number_of_assets': int(row['asset_count']),
                    'total_market_cap': int(row['total_market_cap']) if pd.notnull(row['total_market_cap']) else None,
                    'avg_pe_ratio': float(row['avg_pe_ratio']) if pd.notnull(row['avg_pe_ratio']) else None,
                    'avg_dividend_yield': float(row['avg_dividend_yield']) if pd.notnull(row['avg_dividend_yield']) else None
                })
        
        # Insert using database module
        if sector_records:
            # Convert to format for database insertion (sector_performance table doesn't have a direct insert method)
            try:
                cursor = self.db.connection.cursor()
                
                insert_sql = """
                INSERT OR REPLACE INTO sector_performance 
                (sector, date, number_of_assets, total_market_cap, avg_pe_ratio, avg_dividend_yield)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                
                for record in sector_records:
                    cursor.execute(insert_sql, (
                        record['sector'],
                        record['date'],
                        record['number_of_assets'],
                        record['total_market_cap'],
                        record['avg_pe_ratio'],
                        record['avg_dividend_yield']
                    ))
                
                self.db.connection.commit()
                logger.info(f"✓ Updated sector performance for {len(sector_records)} sectors")
                
            except Exception as e:
                logger.error(f"✗ Error updating sector performance: {e}")
    
    def run_comprehensive_collection(self, symbols_and_types: List[Tuple[str, str]], 
                                   include_economic_data: bool = True,
                                   include_volatility: bool = True,
                                   delay: float = 12.0):
        """
        Run comprehensive data collection for multiple assets
        
        Args:
            symbols_and_types (List[Tuple[str, str]]): List of (symbol, asset_type) tuples
            include_economic_data (bool): Whether to collect economic indicators
            include_volatility (bool): Whether to calculate volatility data
            delay (float): Delay between API calls (seconds)
        """
        logger.info(f"Starting comprehensive data collection for {len(symbols_and_types)} assets...")
        
        successful_assets = 0
        total_assets = len(symbols_and_types)
        
        # Collect asset data with rate limiting
        for i, (symbol, asset_type) in enumerate(symbols_and_types):
            logger.info(f"Processing asset {i+1}/{total_assets}: {symbol}")
            
            success = self.collect_and_insert_asset(symbol, asset_type)
            if success:
                successful_assets += 1
                
                # Collect volatility data if requested
                if include_volatility:
                    self.collect_and_insert_volatility_data(symbol)
            
            # Rate limiting (don't delay after last item)
            if i < total_assets - 1 and delay > 0:
                logger.info(f"Waiting {delay} seconds for rate limiting...")
                import time
                time.sleep(delay)
        
        # Collect economic indicators if requested
        if include_economic_data:
            self.collect_and_insert_economic_indicators()
        
        # Update sector performance
        self.update_sector_performance()
        
        # Final summary
        logger.info(f"Collection complete: {successful_assets}/{total_assets} assets processed successfully")
        self.db.get_data_summary()
    
    def run_quick_test(self):
        """
        Run a quick test with a few symbols
        """
        test_symbols = [
            ('AAPL', 'Stock'),
            ('SPY', 'ETF'),
            ('MSFT', 'Stock')
        ]
        
        logger.info("Running quick test with sample symbols...")
        self.run_comprehensive_collection(
            test_symbols, 
            include_economic_data=False,  # Skip for quick test
            include_volatility=True,
            delay=1.0  # Shorter delay for testing
        )
    
    def close(self):
        """Close database connection"""
        self.db.close()
    
    # Helper methods for data mapping
    def _map_alpha_vantage_to_asset(self, overview_data: Dict, asset_type: str) -> Dict:
        """Map Alpha Vantage data to asset dictionary for database insertion"""
        
        def clean_numeric(value):
            if value in ['None', '-', 'N/A', '', None]:
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        def clean_date(value):
            if value and value != 'None':
                try:
                    from datetime import datetime
                    return datetime.strptime(value, '%Y-%m-%d').date()
                except ValueError:
                    return None
            return None
        
        return {
            'Symbol': overview_data.get('Symbol'),
            'Name': overview_data.get('Name'),
            'Description': overview_data.get('Description'),
            'CIK': overview_data.get('CIK'),
            'Exchange': overview_data.get('Exchange'),
            'Currency': overview_data.get('Currency', 'USD'),
            'Country': overview_data.get('Country'),
            'Sector': overview_data.get('Sector'),
            'Industry': overview_data.get('Industry'),
            'AssetType': overview_data.get('AssetType', asset_type),
            'MarketCapitalization': clean_numeric(overview_data.get('MarketCapitalization')),
            'EBITDA': clean_numeric(overview_data.get('EBITDA')),
            'PERatio': clean_numeric(overview_data.get('PERatio')),
            'PEGRatio': clean_numeric(overview_data.get('PEGRatio')),
            'BookValue': clean_numeric(overview_data.get('BookValue')),
            'DividendPerShare': clean_numeric(overview_data.get('DividendPerShare')),
            'DividendYield': clean_numeric(overview_data.get('DividendYield')),
            'EPS': clean_numeric(overview_data.get('EPS')),
            'RevenuePerShareTTM': clean_numeric(overview_data.get('RevenuePerShareTTM')),
            'ProfitMargin': clean_numeric(overview_data.get('ProfitMargin')),
            'OperatingMarginTTM': clean_numeric(overview_data.get('OperatingMarginTTM')),
            'ReturnOnAssetsTTM': clean_numeric(overview_data.get('ReturnOnAssetsTTM')),
            'ReturnOnEquityTTM': clean_numeric(overview_data.get('ReturnOnEquityTTM')),
            'RevenueTTM': clean_numeric(overview_data.get('RevenueTTM')),
            'GrossProfitTTM': clean_numeric(overview_data.get('GrossProfitTTM')),
            'DilutedEPSTTM': clean_numeric(overview_data.get('DilutedEPSTTM')),
            'QuarterlyEarningsGrowthYOY': clean_numeric(overview_data.get('QuarterlyEarningsGrowthYOY')),
            'QuarterlyRevenueGrowthYOY': clean_numeric(overview_data.get('QuarterlyRevenueGrowthYOY')),
            'AnalystTargetPrice': clean_numeric(overview_data.get('AnalystTargetPrice')),
            'TrailingPE': clean_numeric(overview_data.get('TrailingPE')),
            'ForwardPE': clean_numeric(overview_data.get('ForwardPE')),
            'PriceToSalesRatioTTM': clean_numeric(overview_data.get('PriceToSalesRatioTTM')),
            'PriceToBookRatio': clean_numeric(overview_data.get('PriceToBookRatio')),
            'EVToRevenue': clean_numeric(overview_data.get('EVToRevenue')),
            'EVToEBITDA': clean_numeric(overview_data.get('EVToEBITDA')),
            'Beta': clean_numeric(overview_data.get('Beta')),
            '52WeekHigh': clean_numeric(overview_data.get('52WeekHigh')),
            '52WeekLow': clean_numeric(overview_data.get('52WeekLow')),
            '50DayMovingAverage': clean_numeric(overview_data.get('50DayMovingAverage')),
            '200DayMovingAverage': clean_numeric(overview_data.get('200DayMovingAverage')),
            'SharesOutstanding': clean_numeric(overview_data.get('SharesOutstanding')),
            'DividendDate': clean_date(overview_data.get('DividendDate')),
            'ExDividendDate': clean_date(overview_data.get('ExDividendDate'))
        }
    
    def _map_yfinance_to_asset(self, symbol: str, info: Dict, asset_type: str) -> Dict:
        """Map yfinance data to asset dictionary for database insertion"""
        return {
            'Symbol': symbol,
            'Name': info.get('longName', symbol),
            'Description': info.get('longBusinessSummary', ''),
            'Exchange': info.get('exchange', ''),
            'Currency': info.get('currency', 'USD'),
            'Country': info.get('country', ''),
            'Sector': info.get('sector', ''),
            'Industry': info.get('industry', ''),
            'AssetType': asset_type,
            'MarketCapitalization': info.get('marketCap'),
            'PERatio': info.get('trailingPE'),
            'BookValue': info.get('bookValue'),
            'DividendYield': info.get('dividendYield'),
            'EPS': info.get('trailingEps'),
            'Beta': info.get('beta'),
            '52WeekHigh': info.get('fiftyTwoWeekHigh'),
            '52WeekLow': info.get('fiftyTwoWeekLow'),
            '50DayMovingAverage': info.get('fiftyDayAverage'),
            '200DayMovingAverage': info.get('twoHundredDayAverage'),
            'SharesOutstanding': info.get('sharesOutstanding')
        }
    
    def _create_minimal_asset(self, symbol: str, asset_type: str) -> Dict:
        """Create minimal asset data when no external data is available"""
        return {
            'Symbol': symbol,
            'Name': symbol,
            'AssetType': asset_type,
            'Currency': 'USD'
        }


def create_collection_plan():
    """
    Display a comprehensive data collection plan
    """
    print("=" * 80)
    print("FINANCIAL DATA PIPELINE - COLLECTION PLAN")
    print("=" * 80)
    
    plan = {
        "Phase 1 - Core Market Data (Day 1)": [
            "Major indices: SPY, QQQ, IWM, VTI",
            "Mega-cap stocks: AAPL, MSFT, GOOGL, AMZN, TSLA",
            "Basic price history and fundamentals"
        ],
        "Phase 2 - Sector Coverage (Day 2)": [
            "Sector ETFs: XLK, XLF, XLE, XLV, XLI, etc.",
            "Additional blue chips: NVDA, JPM, JNJ, V, PG",
            "Sector performance calculations"
        ],
        "Phase 3 - Fixed Income & Alternatives (Day 3)": [
            "Bond ETFs: TLT, SHY",
            "Commodities & REITs: GLD, VNQ", 
            "Economic indicators from FRED"
        ],
        "Phase 4 - Analysis & Quality Checks": [
            "Volatility calculations",
            "Data quality validation",
            "Database optimization",
            "Generate summary reports"
        ]
    }
    
    for phase, tasks in plan.items():
        print(f"\n📊 {phase}")
        for task in tasks:
            print(f"   • {task}")
    
    print(f"\n💡 Tips:")
    print(f"   • Respects Alpha Vantage free tier (25 calls/day)")
    print(f"   • Use delay=12 seconds between API calls")
    print(f"   • Total dataset: ~35+ assets with comprehensive data")
    print(f"   • Perfect for portfolio analysis and Tableau dashboards")
    print("=" * 80)


def main():
    """
    Main function demonstrating the financial data pipeline
    """
    print("🚀 FINANCIAL DATA PIPELINE STARTING...")
    print("=" * 60)
    
    # Show collection plan
    create_collection_plan()
    
    # Initialize pipeline
    pipeline = FinancialDataPipeline(
        db_path="FMA/database/financial_markets.db",
        alpha_vantage_key="",  # Replace with your key
        fred_api_key=""             # Replace with your key
    )
    
    # Setup database
    pipeline.setup_database()
    
    # Choose your collection strategy:
    
    # OPTION 1: Quick test (recommended for first run)
    #print("\n🧪 Running quick test...")
    #pipeline.run_quick_test()
    
    # OPTION 2: Phase 1 - Core market data
    phase1_symbols = [
         ('SPY', 'ETF'), ('QQQ', 'ETF'), ('IWM', 'ETF'), ('VTI', 'ETF'),
         ('AAPL', 'Stock'), ('MSFT', 'Stock'), ('GOOGL', 'Stock'), 
         ('AMZN', 'Stock'), ('TSLA', 'Stock')
     ]
    print("\n📊 Running Phase 1 collection...")
    pipeline.run_comprehensive_collection(phase1_symbols, include_economic_data=True)
    
    # OPTION 3: Full recommended dataset (will take several hours with free API limits)
    # recommended_symbols = pipeline.collector.get_recommended_symbols()
    # print(f"\n🏆 Running full collection ({len(recommended_symbols)} assets)...")
    # pipeline.run_comprehensive_collection(recommended_symbols)
    
    # OPTION 4: Economic indicators only (requires FRED API key)
    # print("\n📈 Collecting economic indicators...")
    # pipeline.collect_and_insert_economic_indicators()
    
    # Final database summary
    print("\n" + "=" * 60)
    print("📋 FINAL DATABASE SUMMARY")
    print("=" * 60)
    pipeline.db.get_data_summary()
    
    # Close connections
    pipeline.close()
    
    print("\n✅ Pipeline execution complete!")
    print("💡 Your database is ready for analysis and Tableau visualization!")


def run_custom_collection():
    """
    Example of custom collection workflow
    """
    # Initialize pipeline
    pipeline = FinancialDataPipeline(
        db_path="financial_markets.db",
        alpha_vantage_key="YOUR_ALPHA_VANTAGE_KEY",
        fred_api_key="YOUR_FRED_API_KEY"
    )
    
    # Setup database
    pipeline.setup_database()
    
    # Custom symbol list
    custom_symbols = [
        ('AAPL', 'Stock'),
        ('TSLA', 'Stock'), 
        ('SPY', 'ETF'),
        ('QQQ', 'ETF')
    ]
    
    print("Running custom collection...")
    
    # Collect assets one by one with custom logic
    for symbol, asset_type in custom_symbols:
        print(f"\n--- Processing {symbol} ---")
        
        # Collect and insert asset
        success = pipeline.collect_and_insert_asset(symbol, asset_type)
        
        if success:
            # Add volatility data
            pipeline.collect_and_insert_volatility_data(symbol)
            
            # Check what we have so far
            asset_info = pipeline.db.get_asset_overview(symbol)
            print(f"✓ {symbol}: {asset_info.get('name', 'N/A')} - "
                  f"Market Cap: ${asset_info.get('market_capitalization', 0)/1e9:.1f}B")
        
        # Small delay between assets
        import time
        time.sleep(2)
    
    # Update sector performance
    pipeline.update_sector_performance()
    
    # Show final summary
    pipeline.db.get_data_summary()
    pipeline.close()


if __name__ == "__main__":
    # Run main pipeline
    main()
    
    # Uncomment to run custom collection instead:
    # run_custom_collection()