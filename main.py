#!/usr/bin/env python3
"""
FINANCE BLADI AUTOMATION - FIXED
Resolves:
1. Google Sheets 404 (Force fallback ID)
2. 'No function' errors (Explicitly checks module function names)
"""

import os
import sys
import logging
import traceback
import json
import math
from datetime import datetime
from typing import Dict, Any, List, Optional

# ============================================================================
# SETUP
# ============================================================================

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_MODULES_PATH = os.path.join(PROJECT_ROOT, 'src', 'modules')
sys.path.insert(0, SRC_MODULES_PATH)
sys.path.insert(0, PROJECT_ROOT)

print(f"📍 Project root: {PROJECT_ROOT}")
print(f"📍 Modules path: {SRC_MODULES_PATH}")

for dir_name in ['data', 'downloads', 'logs', 'temp']:
    os.makedirs(os.path.join(PROJECT_ROOT, dir_name), exist_ok=True)

log_file = os.path.join(PROJECT_ROOT, 'logs', f"finance_bladi_{datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA CLEANER
# ============================================================================

class DataCleaner:
    @staticmethod
    def fix_nan_values(data: Any) -> Any:
        if isinstance(data, dict):
            return {k: DataCleaner.fix_nan_values(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [DataCleaner.fix_nan_values(item) for item in data]
        elif isinstance(data, float):
            if math.isnan(data): return ''
            return data
        return data
    
    @staticmethod
    def clean_data_for_export(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = {}
        for key, value in raw_data.items():
            cleaned[key] = DataCleaner.fix_nan_values(value)
        return cleaned

# ============================================================================
# MODULE COLLECTOR (FIXED)
# ============================================================================

class ModuleCollector:
    MODULES = [
        ('bkam_forex', 'bkam_forex'),
        ('bkam_treasury', 'bkam_treasury_official'),
        ('investing_masi', 'investing_masi'),
        ('trading_economics', 'trading_economics'),
        ('yahoo_markets', 'yahoo_markets')
    ]
    
    def __init__(self):
        self.results = {}
        self.errors = []
    
    def collect_all(self) -> Dict[str, Any]:
        logger.info("="*60)
        logger.info("STARTING DATA COLLECTION")
        logger.info("="*60)
        
        for display_name, module_name in self.MODULES:
            try:
                logger.info(f"Collecting {display_name}...")
                result = self._collect_module(display_name, module_name)
                
                if result is not None:
                    result = DataCleaner.fix_nan_values(result)
                    self.results[display_name] = result
                    logger.info(f"✅ {display_name}: Success")
                else:
                    logger.warning(f"⚠️ {display_name}: No data returned")
                    self.errors.append(f"{display_name}: No data")
            except Exception as e:
                error_msg = f"{display_name}: {str(e)}"
                logger.error(f"❌ {error_msg}")
                self.errors.append(error_msg)
        
        logger.info(f"Collection complete: {len(self.results)}/{len(self.MODULES)} modules succeeded")
        return self.results
    
    def _collect_module(self, display_name: str, module_name: str) -> Any:
        try:
            module = __import__(module_name)
            
            # FIXED: Explicitly check for known function names to avoid "No function" error
            if hasattr(module, 'get_bkam_treasury_official'): return module.get_bkam_treasury_official()
            if hasattr(module, 'get_bkam_forex_rates'): return module.get_bkam_forex_rates()
            if hasattr(module, 'get_phosphate_price'): return module.get_phosphate_price()
            
            # Standard generic names
            if hasattr(module, 'collect_data'): return module.collect_data()
            if hasattr(module, 'main'): return module.main()
            if hasattr(module, 'run'): return module.run()
            if hasattr(module, 'get_data'): return module.get_data()
            
            # Fallback search (only if explicit checks failed)
            for attr_name in dir(module):
                if not attr_name.startswith('_'):
                    attr = getattr(module, attr_name)
                    if callable(attr):
                        try: return attr()
                        except: continue
            
            raise Exception(f"No executable function found in {module_name}")
        except Exception as e:
            raise Exception(f"Module error: {str(e)}")

# ============================================================================
# DATA PROCESSOR
# ============================================================================

class DataProcessor:
    COLUMNS = [
        'Date',
        'EUR/MAD', 'USD/MAD',
        'BT2Y (%)', 'BT5Y (%)', 'BT10Y (%)',
        'MASI',
        'Phosphate DAP (USD/T)',
        'BRENT (USD)', 'WTI (USD)', 'GOLD (USD)', 'SILVER (USD)', 'BITCOIN (USD)',
        'EUR/USD', 'USD/JPY', 'GBP/USD',
        'S&P 500', 'Dow Jones', 'NASDAQ',
        'US 10Y Yield (%)', 'VIX'
    ]
    
    @staticmethod
    def _extract_nested_value(data: Any, *paths: List[str]) -> Any:
        if not isinstance(data, dict): return ''
        for path in paths:
            try:
                keys = path.split('.')
                current = data
                for key in keys:
                    if isinstance(current, dict) and key in current: current = current[key]
                    else:
                        current = None
                        break
                if current is not None and current != '':
                    if isinstance(current, float) and math.isnan(current): return ''
                    return current
            except: continue
        return ''

    # ✅ NEW: Helper to auto-correct formatting errors
    @staticmethod
    def _sanitize_float(value, min_val=0, max_val=1000000, divider_correction=10000):
        try:
            if value is None or value == '': return ''
            f_val = float(value)
            
            # Fix scaling error (e.g. 107602 -> 10.7602)
            if f_val > 100 and divider_correction:
                 # Specific check for Forex (usually < 20)
                 if max_val < 50 and f_val > 1000:
                     f_val = f_val / divider_correction
            
            return f_val
        except:
            return value

    def process(self, raw_data: Dict[str, Any]) -> List[List[Any]]:
        today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row = [today]
        raw_data = DataCleaner.fix_nan_values(raw_data)
        
        # 1. Forex (Add Sanity Check: max value 20)
        eur = self._extract_nested_value(raw_data.get('bkam_forex', {}), 'EUR/MAD', 'eur_mad')
        usd = self._extract_nested_value(raw_data.get('bkam_forex', {}), 'USD/MAD', 'usd_mad')
        
        row.append(self._sanitize_float(eur, max_val=20))
        row.append(self._sanitize_float(usd, max_val=20))
        
        # 2. Treasury
        treasury = raw_data.get('bkam_treasury', {})
        row.append(treasury.get('bt2y', ''))
        row.append(treasury.get('bt5y', ''))
        row.append(treasury.get('bt10y', ''))
        
        # 3. MASI
        masi = self._extract_nested_value(raw_data.get('investing_masi', {}), 'MASI', 'masi', 'value')
        if isinstance(masi, str): masi = masi.replace(',', '')
        row.append(masi)
        
        # 4. Phosphate
        row.append(self._extract_nested_value(raw_data.get('trading_economics', {}), 'Phosphate DAP', 'PHOSPHATE_DAP'))
        
        # 5. Yahoo
        yahoo = raw_data.get('yahoo_markets', {})
        yahoo_keys = [
            ['BRENT', 'brent'], ['WTI', 'wti'], ['GOLD', 'gold'], ['SILVER', 'silver'], ['BITCOIN', 'bitcoin'],
            ['EURUSD', 'eurusd'], ['USDJPY', 'usdjpy'], ['GBPUSD', 'gbpusd'],
            ['SP500', '^GSPC'], ['DJIA', '^DJI'], ['NASDAQ', '^IXIC'], ['US10Y', '^TNX'], ['VIX', '^VIX']
        ]
        for keys in yahoo_keys:
            val = self._extract_nested_value(yahoo, *keys)
            row.append(val)
            
        return [[str(c) if c is not None and c != '' else '' for c in row]]

# ============================================================================
# GOOGLE SHEETS EXPORTER (FIXED FOR 'GOOGLE_CREDENTIALS' SECRET)
# ============================================================================

class GoogleSheetsExporter:
    def __init__(self):
        # Priority logic for Spreadsheet ID
        env_id = os.environ.get('SPREADSHEET_ID')
        hardcoded_id = '1unwXUkxs7boI1I29iumlJd3E9WcK9BngF_D4NFWDb90'
        
        if env_id and str(env_id).strip():
            self.spreadsheet_id = str(env_id).strip()
        else:
            self.spreadsheet_id = hardcoded_id
            
        self.sheet_name = 'Finance Bladi'
        self.headers = DataProcessor.COLUMNS
    
    def export(self, data_row: List[List[Any]]) -> bool:
        try:
            print("\n📤 Connecting to Google Sheets...")
            import gspread
            from google.oauth2.service_account import Credentials
            
            scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            credentials = None

            # --- 1. Load from Secret (GOOGLE_CREDENTIALS) ---
            # This matches your existing secret name
            secret_creds = os.environ.get('GOOGLE_CREDENTIALS')
            
            if secret_creds:
                try:
                    print("🔑 Loading credentials from 'GOOGLE_CREDENTIALS' environment variable...")
                    creds_dict = json.loads(secret_creds)
                    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
                except json.JSONDecodeError:
                    print("❌ Error: 'GOOGLE_CREDENTIALS' is not valid JSON. Please check your GitHub Secret content.")
                    return False
            
            # --- 2. Fallback: Local File (for local testing only) ---
            if not credentials:
                print("⚠️ Secret not found. Checking local files...")
                possible_paths = [
                    'credentials.json',
                    os.path.join(PROJECT_ROOT, 'credentials.json'),
                    os.path.join(os.getcwd(), 'credentials.json')
                ]
                for p in possible_paths:
                    if os.path.exists(p):
                        print(f"✅ Found local file: {p}")
                        credentials = Credentials.from_service_account_file(p, scopes=scopes)
                        break
            
            if not credentials:
                print("❌ Authentication failed: No valid credentials found in GOOGLE_CREDENTIALS secret or local file.")
                return False

            # --- 3. Update Sheet ---
            client = gspread.authorize(credentials)
            print(f"📄 Opening spreadsheet: {self.spreadsheet_id}")
            spreadsheet = client.open_by_key(self.spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet(self.sheet_name)
            except:
                print(f"📝 Creating new sheet: '{self.sheet_name}'")
                worksheet = spreadsheet.add_worksheet(title=self.sheet_name, rows=1000, cols=len(self.headers))
            
            self._ensure_headers(worksheet)
            
            # Update Logic
            all_data = worksheet.get_all_values()
            today_date = datetime.now().strftime('%Y-%m-%d')
            
            # Find if today exists (Column A)
            row_index = None
            for i, row in enumerate(all_data[1:], start=2):
                if row and row[0].startswith(today_date):
                    row_index = i
                    break
            
            clean_row = data_row[0]
            
            if row_index:
                print(f"📝 Updating existing row {row_index} for {today_date}...")
                # Update specific range to save API calls
                cell_list = worksheet.range(f"A{row_index}:V{row_index}") # Adjust 'V' to your last column letter
                for i, cell in enumerate(cell_list):
                    if i < len(clean_row):
                        cell.value = clean_row[i]
                worksheet.update_cells(cell_list)
                print(f"✅ Updated row {row_index}")
            else:
                print(f"📝 Adding new row for {today_date}...")
                worksheet.append_row(clean_row)
                print(f"✅ Appended new row")
                
            return True
            
        except Exception as e:
            print(f"❌ Google Sheets export failed: {e}")
            traceback.print_exc()
            return False

    def _ensure_headers(self, worksheet) -> bool:
        try:
            first_row = worksheet.row_values(1)
            if not first_row or first_row[0] != 'Date':
                worksheet.clear()
                worksheet.append_row(self.headers)
            return True
        except: return False

# ============================================================================
# LOCAL STORAGE
# ============================================================================

class LocalStorage:
    def __init__(self):
        self.data_dir = os.path.join(PROJECT_ROOT, 'data')
        os.makedirs(self.data_dir, exist_ok=True)
    
    def save(self, raw_data: Dict, processed_row: List[List[Any]]):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        with open(os.path.join(self.data_dir, f'raw_{timestamp}.json'), 'w') as f:
            json.dump(raw_data, f, indent=2, default=str)
        
        with open(os.path.join(self.data_dir, f'data_{timestamp}.csv'), 'w') as f:
            f.write(','.join(DataProcessor.COLUMNS) + '\n')
            f.write(','.join([str(c) for c in processed_row[0]]) + '\n')

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("\n" + "="*60)
    print("🚀 FINANCE BLADI AUTOMATION")
    print("="*60)
    
    collector = ModuleCollector()
    processor = DataProcessor()
    sheets_exporter = GoogleSheetsExporter()
    local_storage = LocalStorage()
    
    print("📦 Collecting data from modules...")
    raw_data = collector.collect_all()
    
    if not raw_data:
        print("❌ No data collected.")
        return False
        
    print("\n🔄 Processing data...")
    processed_data = processor.process(raw_data)
    
    print("\n" + "="*60)
    export_success = sheets_exporter.export(processed_data)
    
    print("\n💾 Saving local backup...")
    local_storage.save(raw_data, processed_data)
    
    return export_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)