# modules/investing_masi.py - Updated version
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def collect_data(config=None):
    """Collect MASI index data from Investing.com"""
    try:
        print("Collecte MASI depuis Investing.com...")
        
        # Updated URL for Moroccan stock market
        url = "https://www.investing.com/indices/masi"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try to find MASI price (Investing.com structure may vary)
            price_element = soup.find('div', {'data-test': 'instrument-price-last'})
            
            if price_element:
                price = price_element.text.strip()
                print(f"  ✓ MASI: {price}")
                
                # Create result
                result = {
                    'MASI': price,
                    'timestamp': datetime.now().isoformat(),
                    'source': 'Investing.com',
                    'url': url
                }
                
                return result
            else:
                # Alternative search for price
                for element in soup.find_all('span', class_='text-2xl'):
                    if 'MAD' in element.text or any(char.isdigit() for char in element.text):
                        price = element.text.strip()
                        print(f"  ✓ MASI: {price}")
                        return {
                            'MASI': price,
                            'timestamp': datetime.now().isoformat(),
                            'source': 'Investing.com',
                            'url': url
                        }
                
                print("  ⚠️ Prix MASI non trouvé dans la page")
                return {'error': 'Price not found', 'status': 'partial'}
        
        else:
            print(f"  ❌ Erreur MASI: HTTP {response.status_code}")
            return {'error': f'HTTP {response.status_code}', 'status': 'failed'}
            
    except Exception as e:
        print(f"  ❌ Erreur MASI: {str(e)}")
        return {'error': str(e), 'status': 'failed'}

# For backward compatibility
def main():
    return collect_data()

if __name__ == "__main__":
    result = collect_data()
    print(result)