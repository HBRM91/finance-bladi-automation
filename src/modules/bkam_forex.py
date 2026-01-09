import pandas as pd
from io import StringIO
import logging
import sys
import os

# Ensure we can import from src.utils
# (Assuming script runs from project root)
sys.path.append(os.getcwd()) 

try:
    from src.utils.scraper_utils import fetch_url
except ImportError:
    # Fallback if running module directly
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from src.utils.scraper_utils import fetch_url

logger = logging.getLogger(__name__)

def get_bkam_forex_rates():
    """
    Fetches official EUR/MAD and USD/MAD rates.
    Includes: 403 Bypass, 'thousands=None' fix, and Sanity Check.
    """
    url = "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-des-changes/Cours-de-change/Cours-de-reference"
    
    response = fetch_url(url)
    if not response: return None

    try:
        # 1. Parse HTML (Force string to avoid comma/thousands confusion)
        dfs = pd.read_html(StringIO(response.text), thousands=None, decimal=',')
        if not dfs: return None
        df = dfs[0].astype(str)
        
        eur_val = None
        usd_val = None
        rate_col_idx = 1 # Usually 2nd column
        
        for index, row in df.iterrows():
            label = str(row.iloc[0]).upper()
            try:
                # 2. Clean Data
                raw_val = row.iloc[rate_col_idx]
                clean_val_str = raw_val.replace(',', '.').replace('\xa0', '').strip()
                val = float(clean_val_str)
                
                # 3. Sanity Check (Fix 107602 -> 10.7602)
                if val > 100: 
                    val = val / 10000.0
                
                if 'EURO' in label: eur_val = val
                elif 'DOLLAR U.S' in label: usd_val = val

            except: continue
                
        logger.info(f"✅ BKAM Forex: EUR={eur_val}, USD={usd_val}")
        return {'eur_mad': eur_val, 'usd_mad': usd_val}

    except Exception as e:
        logger.error(f"❌ Forex Parsing Error: {e}")
        return None