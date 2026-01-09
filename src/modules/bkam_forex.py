import requests
import pandas as pd
from datetime import datetime
from io import StringIO
import logging

logger = logging.getLogger(__name__)

def get_bkam_forex_rates():
    """
    Fetches official EUR/MAD and USD/MAD rates.
    Fixes the '107602' formatting error by forcing string parsing.
    """
    url = "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-des-changes/Cours-de-change/Cours-de-reference"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'
    }

    try:
        logger.info(f"Connecting to BKAM Forex: {url}")
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        
        # ✅ CRITICAL FIX: 'thousands=None' prevents 10,7602 becoming 107602
        # We also convert to string immediately to handle the comma manually
        dfs = pd.read_html(StringIO(response.text), thousands=None, decimal=',')
        
        if not dfs: return None
        df = dfs[0]
        
        # Ensure we are working with strings for safety
        df = df.astype(str)
        
        # Default column index for today's rate (usually index 1)
        rate_col_idx = 1
        
        eur_val = None
        usd_val = None
        
        for index, row in df.iterrows():
            label = str(row.iloc[0]).upper()
            raw_val = row.iloc[rate_col_idx]
            
            # Cleaning Logic
            try:
                # Replace comma with dot, remove spaces/invisible chars
                clean_val_str = raw_val.replace(',', '.').replace('\xa0', '').strip()
                val = float(clean_val_str)
                
                # ✅ SANITY CHECK IN MODULE
                # If value is huge (e.g. 10760.2), divide it down
                if val > 100:
                    val = val / 10000.0
                    
            except: continue
            
            if 'EURO' in label:
                eur_val = val
            elif 'DOLLAR U.S' in label or 'DOLLAR US' in label:
                usd_val = val
                
        logger.info(f"✅ BKAM Forex: EUR={eur_val}, USD={usd_val}")
        return {'eur_mad': eur_val, 'usd_mad': usd_val}

    except Exception as e:
        logger.error(f"❌ Forex Error: {e}")
        return None