"""
Financial Markets Data Collection System
Focused solely on data collection from APIs - no database insertion
"""

import yfinance as yf
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinancialDataCollector:
    """
    Data collector for financial markets - collection only, no database operations
    """
    
    def __init__(self, alpha_vantage_key: str = None, fred_api_key: str = None):
        """
        Initialize the data collector
        
        Args:
            alpha_vantage_key (str): Alpha Vantage API key for fundamental data
            fred_api_key (str): FRED API key for economic data
        """
        self.alpha_vantage_key = alpha_vantage_key
        self.fred_api_key = fred_api_key
        self.alpha_vantage_url = "https://www.alphavantage.co/query"
        self.fred_url = "https://api.stlouisfed.org/fred/series/observations"
        
    def get_asset_overview_alpha_vantage(self, symbol: str) -> Optional[Dict]:
        """
        Get comprehensive asset overview from Alpha Vantage OVERVIEW function
        
        Args:
            symbol (str): Stock symbol
            
        Returns:
            Dict or None: Asset overview data
        """
        if not self.alpha_vantage_key:
            logger.warning("Alpha Vantage API key not provided, skipping OVERVIEW data")
            return None
            
        params = {
            'function': 'OVERVIEW',
            'symbol': symbol,
            'apikey': self.alpha_vantage_key
        }
        
        try:
            response = requests.get(self.alpha_vantage_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Check if we got valid data
            if 'Symbol' not in data or 'Note' in data or 'Error Message' in data:
                logger.warning(f"Invalid/limited data for {symbol}: {data.get('Note', data.get('Error Message', 'Unknown error'))}")
                return None
                
            logger.info(f"Successfully collected overview data for {symbol}")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching overview for {symbol}: {e}")
            return None
    
    def get_asset_info_yfinance(self, symbol: str) -> Dict:
        """
        Get basic asset information from Yahoo Finance using yfinance
        
        Args:
            symbol (str): Stock symbol
            
        Returns:
            Dict: Asset information (empty dict if error)
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            logger.info(f"Successfully collected yfinance info for {symbol}")
            return info
        except Exception as e:
            logger.error(f"Error fetching yfinance info for {symbol}: {e}")
            return {}
    
    def get_price_data_yfinance(self, symbol: str, period: str = "2y") -> Optional[pd.DataFrame]:
        """
        Get historical price data from Yahoo Finance using yfinance
        
        Args:
            symbol (str): Stock symbol
            period (str): Time period for data (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            
        Returns:
            pd.DataFrame or None: Historical price data
        """
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period)
            
            if hist.empty:
                logger.warning(f"No price data found for {symbol}")
                return None
                
            # Reset index to get date as a column
            hist.reset_index(inplace=True)
            
            # Standardize column names for database insertion
            hist.rename(columns={
                'Date': 'date',
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price',
                'Volume': 'volume',
                'Dividends': 'dividend_amount',
                'Stock Splits': 'split_coefficient'
            }, inplace=True)
            
            # Add symbol column
            hist['symbol'] = symbol
            
            # Add adjusted_close if not present (use close_price as fallback)
            if 'adjusted_close' not in hist.columns:
                hist['adjusted_close'] = hist['close_price']
            
            logger.info(f"Successfully collected {len(hist)} price records for {symbol}")
            return hist
            
        except Exception as e:
            logger.error(f"Error fetching price data for {symbol}: {e}")
            return None
    
    def get_fred_economic_data(self, series_id: str, start_date: str = None) -> Optional[pd.DataFrame]:
        """
        Get economic data from FRED (Federal Reserve Economic Data)
        
        Args:
            series_id (str): FRED series identifier
            start_date (str): Start date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame or None: Economic indicator data
        """
        if not self.fred_api_key:
            logger.warning("FRED API key not provided, skipping economic data")
            return None
            
        if not start_date:
            start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
        
        params = {
            'series_id': series_id,
            'api_key': self.fred_api_key,
            'file_type': 'json',
            'observation_start': start_date
        }
        
        try:
            response = requests.get(self.fred_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'observations' not in data:
                logger.warning(f"No observations found for {series_id}")
                return None
            
            df = pd.DataFrame(data['observations'])
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df['series_id'] = series_id
            
            # Clean up and return
            result = df[['date', 'series_id', 'value']].dropna()
            logger.info(f"Successfully collected {len(result)} records for {series_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching FRED data for {series_id}: {e}")
            return None
    
    def get_vix_data(self, symbol: str = "^VIX", period: str = "1y") -> Optional[pd.DataFrame]:
        """
        Get VIX volatility data from Yahoo Finance
        
        Args:
            symbol (str): VIX symbol (default: ^VIX)
            period (str): Time period for data
            
        Returns:
            pd.DataFrame or None: VIX data
        """
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period)
            
            if hist.empty:
                logger.warning(f"No VIX data found for {symbol}")
                return None
                
            hist.reset_index(inplace=True)
            hist['symbol'] = symbol.replace('^', '')  # Clean symbol name
            hist['volatility_type'] = 'Implied'
            
            logger.info(f"Successfully collected VIX data: {len(hist)} records")
            return hist
            
        except Exception as e:
            logger.error(f"Error fetching VIX data: {e}")
            return None
    
    def calculate_realized_volatility(self, price_data: pd.DataFrame, window: int = 30) -> pd.DataFrame:
        """
        Calculate realized volatility from price data
        
        Args:
            price_data (pd.DataFrame): Price data with 'close_price' column
            window (int): Rolling window for volatility calculation
            
        Returns:
            pd.DataFrame: Volatility data
        """
        try:
            # Calculate returns
            price_data = price_data.copy()
            price_data['returns'] = price_data['close_price'].pct_change()
            
            # Calculate rolling volatility (annualized)
            price_data[f'volatility_{window}d'] = (
                price_data['returns'].rolling(window=window).std() * (252**0.5)
            )
            
            # Prepare volatility DataFrame
            volatility_df = price_data[['symbol', 'date', f'volatility_{window}d']].copy()
            volatility_df = volatility_df.rename(columns={f'volatility_{window}d': 'volatility_value'})
            volatility_df['volatility_type'] = 'Realized'
            volatility_df['volatility_period'] = window
            
            # Remove NaN values
            volatility_df = volatility_df.dropna()
            
            logger.info(f"Calculated realized volatility: {len(volatility_df)} records")
            return volatility_df
            
        except Exception as e:
            logger.error(f"Error calculating realized volatility: {e}")
            return pd.DataFrame()
    
    def batch_collect_with_delay(self, symbols: List[str], collection_func, delay: float = 12.0, **kwargs):
        """
        Collect data for multiple symbols with rate limiting
        
        Args:
            symbols (List[str]): List of symbols to collect
            collection_func: Function to call for each symbol
            delay (float): Seconds between API calls (Alpha Vantage free: 25 calls/day = 12 sec)
            **kwargs: Additional arguments for collection function
            
        Returns:
            Dict: Results keyed by symbol
        """
        results = {}
        total = len(symbols)
        
        for i, symbol in enumerate(symbols):
            logger.info(f"Collecting data for {symbol} ({i+1}/{total})...")
            
            try:
                result = collection_func(symbol, **kwargs)
                results[symbol] = result
                
                if result is not None:
                    logger.info(f"✓ Successfully collected data for {symbol}")
                else:
                    logger.warning(f"✗ No data collected for {symbol}")
                    
            except Exception as e:
                logger.error(f"✗ Error collecting data for {symbol}: {e}")
                results[symbol] = None
            
            # Rate limiting (don't delay after last item)
            if i < total - 1 and delay > 0:
                logger.info(f"Waiting {delay} seconds for rate limiting...")
                time.sleep(delay)
        
        successful = sum(1 for result in results.values() if result is not None)
        logger.info(f"Batch collection complete: {successful}/{total} successful")
        
        return results
    
    def get_recommended_symbols(self) -> List[Tuple[str, str]]:
        """
        Get recommended symbols for comprehensive financial markets analysis
        
        Returns:
            List[Tuple[str, str]]: List of (symbol, asset_type) tuples
        """
        return [
            # Major Market Indices
            ('SPY', 'ETF'),     # S&P 500
            ('QQQ', 'ETF'),     # NASDAQ 100
            ('IWM', 'ETF'),     # Russell 2000
            ('VTI', 'ETF'),     # Total Stock Market
            
            # Sector ETFs
            ('XLK', 'ETF'),     # Technology
            ('XLF', 'ETF'),     # Financial
            ('XLE', 'ETF'),     # Energy
            ('XLV', 'ETF'),     # Healthcare
            ('XLI', 'ETF'),     # Industrial
            ('XLY', 'ETF'),     # Consumer Discretionary
            ('XLP', 'ETF'),     # Consumer Staples
            ('XLU', 'ETF'),     # Utilities
            ('XLRE', 'ETF'),    # Real Estate
            ('XLB', 'ETF'),     # Materials
            
            # Blue Chip Stocks
            ('AAPL', 'Stock'),  # Apple
            ('MSFT', 'Stock'),  # Microsoft  
            ('GOOGL', 'Stock'), # Alphabet
            ('AMZN', 'Stock'),  # Amazon
            ('TSLA', 'Stock'),  # Tesla
            ('NVDA', 'Stock'),  # NVIDIA
            ('JPM', 'Stock'),   # JPMorgan Chase
            ('JNJ', 'Stock'),   # Johnson & Johnson
            ('V', 'Stock'),     # Visa
            ('PG', 'Stock'),    # Procter & Gamble
            
            # Bonds & Commodities
            ('TLT', 'ETF'),     # 20+ Year Treasury Bonds
            ('SHY', 'ETF'),     # 1-3 Year Treasury Bonds
            ('GLD', 'ETF'),     # Gold
            ('VNQ', 'ETF'),     # REITs
        ]
    
    def get_fred_indicators(self) -> Dict[str, Dict]:
        """
        Get recommended FRED economic indicators
        
        Returns:
            Dict: Dictionary of series_id -> metadata
        """
        return {
            'FEDFUNDS': {
                'name': 'Federal Funds Rate', 
                'unit': 'Percent', 
                'frequency': 'Monthly',
                'source': 'FRED'
            },
            'UNRATE': {
                'name': 'Unemployment Rate', 
                'unit': 'Percent', 
                'frequency': 'Monthly',
                'source': 'FRED'
            }, 
            'CPIAUCSL': {
                'name': 'Consumer Price Index for All Urban Consumers', 
                'unit': 'Index 1982-84=100', 
                'frequency': 'Monthly',
                'source': 'FRED'
            },
            'GDP': {
                'name': 'Gross Domestic Product', 
                'unit': 'Billions of Dollars', 
                'frequency': 'Quarterly',
                'source': 'FRED'
            },
            'GS10': {
                'name': '10-Year Treasury Constant Maturity Rate', 
                'unit': 'Percent', 
                'frequency': 'Daily',
                'source': 'FRED'
            },
            'DGS2': {
                'name': '2-Year Treasury Constant Maturity Rate', 
                'unit': 'Percent', 
                'frequency': 'Daily',
                'source': 'FRED'
            },
            'VIXCLS': {
                'name': 'CBOE Volatility Index: VIX', 
                'unit': 'Index', 
                'frequency': 'Daily',
                'source': 'FRED'
            },
            'UMCSENT': {
                'name': 'University of Michigan: Consumer Sentiment', 
                'unit': 'Index 1966:Q1=100', 
                'frequency': 'Monthly',
                'source': 'FRED'
            }
        }

# Example usage (for testing only - no actual collection)
if __name__ == "__main__":
    # Initialize collector
    collector = FinancialDataCollector(
        alpha_vantage_key="YOUR_ALPHA_VANTAGE_KEY",  # Replace with your key
        fred_api_key="YOUR_FRED_API_KEY"              # Replace with your key
    )
    
    # Test single symbol collection (no database insertion)
    print("Testing data collection (no database operations)...")
    
    # Test Alpha Vantage overview
    overview = collector.get_asset_overview_alpha_vantage('AAPL')
    print(f"Alpha Vantage Overview: {'Success' if overview else 'Failed'}")
    
    # Test Yahoo Finance price data
    price_data = collector.get_price_data_yfinance('AAPL', period='5d')
    print(f"Yahoo Finance Price Data: {'Success' if price_data is not None else 'Failed'}")
    if price_data is not None:
        print(f"  Records: {len(price_data)}")
    
    # Test FRED data (if key provided)
    fred_data = collector.get_fred_economic_data('FEDFUNDS')
    print(f"FRED Economic Data: {'Success' if fred_data is not None else 'Failed/No Key'}")
    
    print("\nData collection module ready for use with main orchestrator!")