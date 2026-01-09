# src/modules/yahoo_markets.py - IMPROVED WITH INVESTING.COM FALLBACK
import yfinance as yf
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import logging
import time
import json

logger = logging.getLogger(__name__)

def get_from_investing(symbol: str) -> float:
    """Get data from Investing.com as fallback"""
    investing_map = {
        'BRENT': 'brent-crude',  # Brent Crude Oil
        'WTI': 'crude-oil',      # WTI Crude Oil
        'GOLD': 'gold',          # Gold
        'SILVER': 'silver',      # Silver
        'COPPER': 'copper',      # Copper
        'SP500': 'us-spx-500',   # S&P 500
        'DJIA': 'us-30',         # Dow Jones
        'NASDAQ': 'nasdaq-composite',  # NASDAQ
        'US10Y': 'us-10y-bond-yield',  # US 10Y Yield
        'VIX': 'vix',            # VIX
        'EURUSD': 'eur-usd',     # EUR/USD
        'USDJPY': 'usd-jpy',     # USD/JPY
        'GBPUSD': 'gbp-usd',     # GBP/USD
        'USDCAD': 'usd-cad',     # USD/CAD
        'AUDUSD': 'aud-usd',     # AUD/USD
        'BITCOIN': 'bitcoin-usd' # Bitcoin
    }
    
    if symbol not in investing_map:
        return None
    
    try:
        url = f"https://www.investing.com/indices/{investing_map[symbol]}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try to find price (Investing.com structure)
            price_element = soup.find('div', {'data-test': 'instrument-price-last'})
            
            if price_element:
                price_text = price_element.text.strip()
                # Clean the price (remove commas, symbols)
                price_text = price_text.replace(',', '').replace('$', '').replace('€', '').replace('£', '')
                
                try:
                    return float(price_text)
                except:
                    return None
        
        return None
        
    except Exception as e:
        logger.warning(f"Investing.com fallback failed for {symbol}: {e}")
        return None

def get_from_yahoo(symbol: str, ticker: str, retries: int = 2) -> float:
    """Get data from Yahoo Finance with retries"""
    for attempt in range(retries):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d")
            
            if not hist.empty:
                return float(hist['Close'].iloc[-1])
            
            # Try 1-minute data if daily fails
            hist = stock.history(period="1d", interval="1m")
            if not hist.empty:
                return float(hist['Close'].iloc[-1])
            
            return None
            
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)  # Wait before retry
                continue
            logger.warning(f"Yahoo Finance failed for {symbol}: {e}")
            return None

def collect_data():
    """Collect market data with Yahoo Finance primary and Investing.com fallback"""
    print("\nCollecte Yahoo Finance avec fallback Investing.com...")
    
    # Asset mapping: Symbol -> (Yahoo Ticker, Description)
    assets = {
        'BRENT': ('BZ=F', 'Brent Crude Oil'),
        'WTI': ('CL=F', 'WTI Crude Oil'),
        'GAS': ('NG=F', 'Natural Gas'),
        'GOLD': ('GC=F', 'Gold'),
        'SILVER': ('SI=F', 'Silver'),
        'COPPER': ('HG=F', 'Copper'),
        'SP500': ('^GSPC', 'S&P 500'),
        'DJIA': ('^DJI', 'Dow Jones'),
        'NASDAQ': ('^IXIC', 'NASDAQ'),
        'RUSSELL2000': ('^RUT', 'Russell 2000'),
        'CAC40': ('^FCHI', 'CAC 40'),
        'DAX': ('^GDAXI', 'DAX'),
        'FTSE100': ('^FTSE', 'FTSE 100'),
        'US10Y': ('^TNX', 'US 10Y Treasury Yield'),
        'VIX': ('^VIX', 'VIX Volatility Index'),
        'BITCOIN': ('BTC-USD', 'Bitcoin'),
        'EURUSD': ('EURUSD=X', 'EUR/USD'),
        'USDJPY': ('JPY=X', 'USD/JPY'),
        'GBPUSD': ('GBPUSD=X', 'GBP/USD'),
        'USDCAD': ('CAD=X', 'USD/CAD'),
        'AUDUSD': ('AUDUSD=X', 'AUD/USD')
    }
    
    results = {
        'YAHOO_EXTRACTION_TIME': datetime.now().isoformat(),
        'YAHOO_SOURCE': 'Yahoo Finance with Investing.com fallback'
    }
    
    successful = 0
    total = len(assets)
    
    for symbol, (ticker, description) in assets.items():
        print(f"  → {symbol}: ", end='')
        
        # Try Yahoo first
        value = get_from_yahoo(symbol, ticker)
        
        # If Yahoo returns None or nan, try Investing.com
        if value is None or (isinstance(value, float) and pd.isna(value)):
            print(f"Yahoo failed, trying Investing.com...", end='')
            value = get_from_investing(symbol)
            
            if value is not None:
                results[symbol] = value
                successful += 1
                print(f"✓ {value} (Investing)")
            else:
                results[symbol] = None
                print(f"✗ Both failed")
        else:
            results[symbol] = value
            successful += 1
            print(f"✓ {value}")
    
    print(f"  → {successful}/{total} actifs récupérés")
    
    # Alternative data source for key assets if many failed
    if successful < total / 2:  # Less than half succeeded
        print("  ⚠ Many assets failed, trying alternative sources...")
        results = get_alternative_data(results)
    
    return results

def get_alternative_data(existing_results: dict) -> dict:
    """Try alternative APIs for critical assets"""
    try:
        # Try Alpha Vantage for stock indices (free tier)
        alpha_vantage_key = 'demo'  # Replace with your API key if available
        
        # S&P 500 from Alpha Vantage
        try:
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=SPY&apikey={alpha_vantage_key}"
            response = requests.get(url, timeout=5).json()
            if 'Global Quote' in response:
                sp500_price = float(response['Global Quote']['05. price'])
                existing_results['SP500'] = sp500_price
        except:
            pass
        
        # Try cryptocurrency APIs for Bitcoin
        try:
            url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
            response = requests.get(url, timeout=5).json()
            if 'bitcoin' in response:
                existing_results['BITCOIN'] = response['bitcoin']['usd']
        except:
            pass
        
        # Try forex API for currency pairs
        try:
            url = "https://api.exchangerate-api.com/v4/latest/USD"
            response = requests.get(url, timeout=5).json()
            if 'rates' in response:
                existing_results['EURUSD'] = 1 / response['rates']['EUR'] if 'EUR' in response['rates'] else None
                existing_results['USDJPY'] = response['rates']['JPY'] if 'JPY' in response['rates'] else None
        except:
            pass
        
    except Exception as e:
        logger.warning(f"Alternative data sources failed: {e}")
    
    return existing_results

def main():
    """For testing the module directly"""
    data = collect_data()
    print("\n📊 Results:")
    for key, value in list(data.items())[:10]:  # Show first 10
        print(f"  {key}: {value}")
    return data

if __name__ == "__main__":
    main()