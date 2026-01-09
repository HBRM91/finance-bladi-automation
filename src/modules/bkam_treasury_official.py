import pandas as pd
from datetime import datetime
from io import StringIO
import logging
import sys
import os

# Import scraper utility
try:
    from src.utils.scraper_utils import fetch_url
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from src.utils.scraper_utils import fetch_url

logger = logging.getLogger(__name__)

def interpolate_linear(target_years, sorted_data):
    """
    Standard Linear Interpolation: Y = Y1 + (x - x1) * (Y2 - Y1) / (X2 - X1)
    """
    if not sorted_data: return None
    
    target_days = int(target_years * 365)
    
    # 1. Boundary Checks
    if target_days <= sorted_data[0]['days']: return sorted_data[0]['rate']
    if target_days >= sorted_data[-1]['days']: return sorted_data[-1]['rate']

    # 2. Find closest neighbors
    p1 = p2 = None
    for i in range(len(sorted_data) - 1):
        if sorted_data[i]['days'] <= target_days <= sorted_data[i+1]['days']:
            p1, p2 = sorted_data[i], sorted_data[i+1]
            break
    
    if not p1 or not p2: return None
    
    # 3. Interpolate
    days_diff = p2['days'] - p1['days']
    rate_diff = p2['rate'] - p1['rate']
    
    if days_diff == 0: return p1['rate']
    
    fraction = (target_days - p1['days']) / days_diff
    interpolated_rate = p1['rate'] + (fraction * rate_diff)
    
    # Log the calculation for transparency
    print(f"\n[CALCUL] BT{target_years}Y ({target_years} ans):")
    print(f"  → Interpolé: {interpolated_rate:.3f}%")
    print(f"  → Bornes: {p1['date'].strftime('%d/%m/%Y')} ({p1['rate']}%) <---> {p2['date'].strftime('%d/%m/%Y')} ({p2['rate']}%)")
    
    return round(interpolated_rate, 3)

def get_bkam_treasury_official():
    url = "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-obligataire/Marche-des-bons-de-tresor/Marche-secondaire/Taux-de-reference-des-bons-du-tresor"
    results = {'bt2y': '', 'bt5y': '', 'bt10y': ''}

    response = fetch_url(url)
    if not response: 
        logger.error("❌ No response from BKAM")
        return results

    try:
        # Read ALL tables on the page
        dfs = pd.read_html(StringIO(response.text))
        
        if not dfs:
            logger.error("❌ No tables found in HTML")
            return results
        
        logger.info(f"🔎 Found {len(dfs)} tables. Scanning for data...")

        # --- SMART TABLE SELECTOR ---
        # We iterate through ALL tables to find the one with 'échéance' and 'taux'
        target_df = None
        date_col = None
        rate_col = None

        for i, df in enumerate(dfs):
            # Normalize column names
            df.columns = [str(c).strip().lower() for c in df.columns]
            
            # Check keywords
            d_col = next((c for c in df.columns if 'chah' in c or 'echeance' in c or 'échéance' in c), None)
            r_col = next((c for c in df.columns if 'taux' in c or 'moyen' in c), None)
            
            if d_col and r_col:
                target_df = df
                date_col = d_col
                rate_col = r_col
                logger.info(f"✅ MATCH: Table #{i} contains '{d_col}' and '{r_col}'. Using this table.")
                break
            else:
                logger.debug(f"Skipping Table #{i}: Columns {df.columns} do not match.")

        if target_df is None:
            logger.error("❌ Could not identify the Treasury table. Website structure might have changed.")
            return results

        # --- DATA EXTRACTION ---
        today = datetime.now()
        data_points = []

        for index, row in target_df.iterrows():
            try:
                # 1. Parse Date
                date_val = str(row[date_col])
                maturity = pd.to_datetime(date_val, dayfirst=True, errors='coerce')
                
                if pd.isna(maturity): continue
                
                # 2. Parse Rate (Handle French format "2,210 %")
                raw_rate = str(row[rate_col]).replace(',', '.').replace('%', '').strip()
                rate = float(raw_rate)
                
                # 3. Calculate Days
                days = (maturity - today).days
                if days <= 0: continue # Skip expired

                data_points.append({'days': days, 'rate': rate, 'date': maturity})
            except Exception:
                continue
        
        # Sort by duration for interpolation
        data_points.sort(key=lambda x: x['days'])
        
        if not data_points:
            logger.warning("⚠️ Table found but no valid rows extracted.")
            return results

        print("="*50)
        print(f"📊 {len(data_points)} Lignes extraites. Lancement de l'interpolation...")
        print("="*50)

        # --- INTERPOLATION ---
        results['bt2y'] = interpolate_linear(2.0, data_points)
        results['bt5y'] = interpolate_linear(5.0, data_points)
        results['bt10y'] = interpolate_linear(10.0, data_points)
        
        return results

    except Exception as e:
        logger.error(f"❌ Treasury Module Error: {e}")
        return results