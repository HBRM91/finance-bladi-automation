import requests
import pandas as pd
from datetime import datetime
from io import StringIO
import logging

logger = logging.getLogger(__name__)

def interpolate_linear(target_years, sorted_data):
    """
    Performs linear interpolation for a specific target duration.
    target_years: 2, 5, or 10
    sorted_data: list of dicts [{'days': int, 'rate': float, 'date': str}]
    """
    target_days = int(target_years * 365) # Standard convention (or 365.25)
    
    # 1. Handle edge cases (Target lower than min or higher than max)
    if not sorted_data:
        return None
    
    if target_days <= sorted_data[0]['days']:
        return sorted_data[0]['rate'] # Too short, take minimum
    if target_days >= sorted_data[-1]['days']:
        return sorted_data[-1]['rate'] # Too long, take maximum

    # 2. Find the two points surrounding the target (P1 and P2)
    p1 = None
    p2 = None

    for i in range(len(sorted_data) - 1):
        if sorted_data[i]['days'] <= target_days <= sorted_data[i+1]['days']:
            p1 = sorted_data[i]
            p2 = sorted_data[i+1]
            break
    
    if not p1 or not p2:
        return None

    # 3. Mathematical Interpolation Formula
    # y = y1 + (x - x1) * (y2 - y1) / (x2 - x1)
    
    days_diff = p2['days'] - p1['days']
    rate_diff = p2['rate'] - p1['rate']
    
    if days_diff == 0: return p1['rate']

    fraction = (target_days - p1['days']) / days_diff
    interpolated_rate = p1['rate'] + (fraction * rate_diff)

    # 4. Log the math (As requested)
    log_msg = (
        f"\nBT{target_years}Y ({target_years} ans):\n"
        f"  → Taux: {interpolated_rate:.3f}%\n"
        f"  → Méthode: Interpolation linéaire\n"
        f"  → Interpolation entre:\n"
        f"     • {p1['date']}: {p1['rate']}% ({p1['days']} jours)\n"
        f"     • {p2['date']}: {p2['rate']}% ({p2['days']} jours)\n"
        f"  → Maturité réelle: {target_years} ans ({target_days} jours)"
    )
    print(log_msg) # Print to console for immediate visibility
    logger.info(f"Interpolation {target_years}Y: {interpolated_rate:.3f}% (between {p1['days']}d and {p2['days']}d)")

    return round(interpolated_rate, 3)

def get_bkam_treasury_official():
    url = "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-obligataire/Marche-des-bons-de-tresor/Marche-secondaire/Taux-de-reference-des-bons-du-tresor"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'
    }

    results = {'bt2y': '', 'bt5y': '', 'bt10y': ''}

    try:
        logger.info(f"Connecting to BKAM Treasury: {url}")
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        
        # Parse HTML
        dfs = pd.read_html(StringIO(response.text))
        if not dfs: return results
        df = dfs[0]
        
        # Clean columns
        df.columns = [str(c).strip().lower() for c in df.columns]
        date_col = next((c for c in df.columns if 'chah' in c or 'echeance' in c or 'échéance' in c), None)
        rate_col = next((c for c in df.columns if 'taux' in c or 'moyen' in c), None)
        
        if not date_col or not rate_col: return results

        today = datetime.now()
        data_points = []

        # 1. EXTRACT ALL RAW DATA
        for index, row in df.iterrows():
            try:
                # Get Date
                date_val = str(row[date_col])
                maturity = pd.to_datetime(date_val, dayfirst=True, errors='coerce')
                if pd.isna(maturity): continue
                
                # Get Rate
                raw_rate = str(row[rate_col]).replace(',', '.').replace('%', '').strip()
                rate = float(raw_rate)
                
                # Calculate Days
                days = (maturity - today).days
                if days <= 0: continue

                data_points.append({
                    'days': days,
                    'rate': rate,
                    'date': maturity.strftime('%d/%m/%Y')
                })
            except: continue
        
        # 2. SORT BY DAYS (Required for interpolation)
        data_points.sort(key=lambda x: x['days'])
        
        if not data_points:
            logger.warning("No valid data points extracted for interpolation")
            return results

        print("="*60)
        print("CALCUL DES TAUX STANDARD 2Y, 5Y, 10Y (INTERPOLATION)")
        print("="*60)

        # 3. COMPUTE INTERPOLATED RATES
        results['bt2y'] = interpolate_linear(2.0, data_points)
        results['bt5y'] = interpolate_linear(5.0, data_points)
        results['bt10y'] = interpolate_linear(10.0, data_points)
        
        print("="*60)
        
        return results

    except Exception as e:
        logger.error(f"Error fetching BKAM Treasury: {e}")
        return results

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    get_bkam_treasury_official()