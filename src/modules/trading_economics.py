# src/modules/trading_economics.py
import requests
from datetime import datetime as dt
import re

def get_phosphate_price():
    """
    Récupère le prix du Phosphate DAP depuis TradingEconomics.
    """
    print("Collecte Phosphate DAP depuis TradingEconomics...")
    
    try:
        url = "https://tradingeconomics.com/commodity/di-ammonium"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}")
        
        html_content = response.text
        
        # Chercher la valeur dans le tableau (selon votre structure HTML)
        # Pattern pour: <td>625.00</td> (valeur 'Actual')
        pattern = r'<td>(\d+\.?\d*)</td>\s*<td>(\d+\.?\d*)</td>'
        
        matches = re.findall(pattern, html_content)
        
        if matches and len(matches) > 0:
            # Prendre la première valeur (Actual)
            phosphate_value = float(matches[0][0])
            print(f"  ✓ Phosphate DAP: {phosphate_value} USD/T")
            return {'PHOSPHATE_DAP': phosphate_value, 'PHOSPHATE_SOURCE': 'TradingEconomics'}
        else:
            # Autre pattern
            pattern2 = r'"price":"([\d\.]+)"'
            match = re.search(pattern2, html_content)
            if match:
                phosphate_value = float(match.group(1))
                print(f"  ✓ Phosphate DAP (pattern2): {phosphate_value} USD/T")
                return {'PHOSPHATE_DAP': phosphate_value, 'PHOSPHATE_SOURCE': 'TradingEconomics'}
        
        print("  ⚠️  Phosphate non trouvé")
        return {'PHOSPHATE_DAP': None}
        
    except Exception as e:
        print(f"  ❌ Erreur Phosphate: {e}")
        return {'PHOSPHATE_DAP': None}