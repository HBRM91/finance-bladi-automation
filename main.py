{
type: "file_update",
fileName: "main.py",
content: """#!/usr/bin/env python3
\"\"\"
FINANCE BLADI AUTOMATION - FIXED VERSION
Handles credentials path issues, empty env vars, and improves debugging
\"\"\"

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

# Get project root
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Add src/modules to Python path
SRC_MODULES_PATH = os.path.join(PROJECT_ROOT, 'src', 'modules')
sys.path.insert(0, SRC_MODULES_PATH)
sys.path.insert(0, PROJECT_ROOT)

print(f"📍 Project root: {PROJECT_ROOT}")
print(f"📍 Modules path: {SRC_MODULES_PATH}")

# Create directories
for dir_name in ['data', 'downloads', 'logs', 'temp']:
    dir_path = os.path.join(PROJECT_ROOT, dir_name)
    os.makedirs(dir_path, exist_ok=True)

# Setup logging
log_file = os.path.join(PROJECT_ROOT, 'logs', f"finance_bladi_{datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA CLEANER
# ============================================================================

class DataCleaner:
    \"\"\"Cleans and fixes data before export\"\"\"
    
    @staticmethod
    def fix_nan_values(data: Any) -> Any:
        \"\"\"Replace nan values with empty strings\"\"\"
        if isinstance(data, dict):
            return {k: DataCleaner.fix_nan_values(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [DataCleaner.fix_nan_values(item) for item in data]
        elif isinstance(data, float):
            if math.isnan(data):
                return ''
            return data
        else:
            return data
    
    @staticmethod
    def clean_data_for_export(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        \"\"\"Clean entire dataset for Google Sheets\"\"\"
        cleaned = {}
        for key, value in raw_data.items():
            cleaned[key] = DataCleaner.fix_nan_values(value)
        return cleaned

# ============================================================================
# MODULE COLLECTOR
# ============================================================================

class ModuleCollector:
    \"\"\"Collects data from all financial modules\"\"\"
    
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
        \"\"\"Collect data from all modules\"\"\"
        logger.info("="*60)
        logger.info("STARTING DATA COLLECTION")
        logger.info("="*60)
        
        for display_name, module_name in self.MODULES:
            try:
                logger.info(f"Collecting {display_name}...")
                result = self._collect_module(display_name, module_name)
                
                if result is not None:
                    # Clean the result immediately
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
            
            # Function discovery logic
            if hasattr(module, 'get_bkam_treasury_official'): # Specific check for treasury
                return module.get_bkam_treasury_official()
            elif hasattr(module, 'get_bkam_forex_rates'): # Specific check for forex
                return module.get_bkam_forex_rates()
            elif hasattr(module, 'collect_data'):
                return module.collect_data()
            elif hasattr(module, 'main'):
                return module.main()
            elif hasattr(module, 'run'):
                return module.run()
            elif hasattr(module, 'get_data'):
                return module.get_data()
            else:
                for attr_name in dir(module):
                    if not attr_name.startswith('_'):
                        attr = getattr(module, attr_name)
                        if callable(attr):
                            try:
                                return attr()
                            except:
                                continue
                raise Exception(f"No executable function found in {module_name}")
                
        except Exception as e:
            raise Exception(f"Module error: {str(e)}")

# ============================================================================
# DATA PROCESSOR
# ============================================================================

class DataProcessor:
    \"\"\"Processes raw data into unified format for Google Sheets\"\"\"
    
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
        if not isinstance(data, dict):
            return ''
        
        for path in paths:
            try:
                keys = path.split('.')
                current = data
                for key in keys:
                    if isinstance(current, dict) and key in current:
                        current = current[key]
                    else:
                        current = None
                        break
                
                if current is not None and current != '':
                    if isinstance(current, float) and math.isnan(current):
                        return ''
                    return current
            except (KeyError, AttributeError, TypeError):
                continue
        return ''
    
    @staticmethod
    def _clean_masi_value(value: Any) -> Any:
        if isinstance(value, str):
            return value.replace(',', '')
        return value
    
    def process(self, raw_data: Dict[str, Any]) -> List[List[Any]]:
        today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row = [today]
        
        raw_data = DataCleaner.fix_nan_values(raw_data)
        
        # 1. EUR/MAD
        eur_mad = self._extract_nested_value(raw_data.get('bkam_forex', {}), 'EUR/MAD', 'eur_mad', 'EUR_MAD')
        row.append(eur_mad if eur_mad not in ['', None] else '')
        
        # 2. USD/MAD
        usd_mad = self._extract_nested_value(raw_data.get('bkam_forex', {}), 'USD/MAD', 'usd_mad', 'USD_MAD')
        row.append(usd_mad if usd_mad not in ['', None] else '')
        
        # 3-5. Treasury rates
        treasury_data = raw_data.get('bkam_treasury', {})
        row.append(self._extract_nested_value(treasury_data, 'BT2Y', 'bt2y', '2Y'))
        row.append(self._extract_nested_value(treasury_data, 'BT5Y', 'bt5y', '5Y'))
        row.append(self._extract_nested_value(treasury_data, 'BT10Y', 'bt10y', '10Y'))
        
        # 6. MASI
        masi_value = self._extract_nested_value(raw_data.get('investing_masi', {}), 'MASI', 'masi', 'value')
        row.append(self._clean_masi_value(masi_value) if masi_value not in ['', None] else '')
        
        # 7. Phosphate DAP
        phosphate_value = self._extract_nested_value(raw_data.get('trading_economics', {}), 'Phosphate DAP', 'phosphate', 'DAP', 'value', 'PHOSPHATE_DAP')
        row.append(phosphate_value if phosphate_value not in ['', None] else '')
        
        # 8-20. Yahoo Finance data
        yahoo_data = raw_data.get('yahoo_markets', {})
        yahoo_extractors = [
            ('BRENT', ['BRENT', 'brent']), ('WTI', ['WTI', 'wti']),
            ('GOLD', ['GOLD', 'gold', 'XAU']), ('SILVER', ['SILVER', 'silver', 'XAG']),
            ('BITCOIN', ['BITCOIN', 'bitcoin', 'BTC']), ('EURUSD', ['EURUSD', 'eurusd', 'EUR/USD']),
            ('USDJPY', ['USDJPY', 'usdjpy', 'USD/JPY']), ('GBPUSD', ['GBPUSD', 'gbpusd', 'GBP/USD']),
            ('SP500', ['SP500', 'sp500', 'S&P500', '^GSPC']), ('DJIA', ['DJIA', 'dow', 'DOW', '^DJI']),
            ('NASDAQ', ['NASDAQ', 'nasdaq', '^IXIC', 'NAS100']), ('US10Y', ['US10Y', 'us10y', '10Y', 'UST10Y']),
            ('VIX', ['VIX', 'vix', '^VIX'])
        ]
        
        for _, keys in yahoo_extractors:
            value = self._extract_nested_value(yahoo_data, *keys)
            if isinstance(value, float) and math.isnan(value):
                row.append('')
            else:
                row.append(value if value not in ['', None] else '')
        
        cleaned_row = []
        for cell in row:
            if isinstance(cell, float) and math.isnan(cell):
                cleaned_row.append('')
            elif cell is None:
                cleaned_row.append('')
            else:
                cleaned_row.append(cell)
        
        return [cleaned_row]

# ============================================================================
# GOOGLE SHEETS EXPORTER - FIXED
# ============================================================================

class GoogleSheetsExporter:
    \"\"\"Exports data to Google Sheets - FIXED credentials and ID logic\"\"\"
    
    def __init__(self, credentials_path: str = None):
        # FIX: Check if env var exists AND is not empty, otherwise use default
        env_id = os.environ.get('SPREADSHEET_ID')
        if env_id and env_id.strip():
            self.spreadsheet_id = env_id
            print(f"📋 Using Spreadsheet ID from environment: {self.spreadsheet_id[:5]}...")
        else:
            self.spreadsheet_id = '1unwXUkxs7boI1I29iumlJd3E9WcK9BngF_D4NFWDb90'
            print(f"📋 Using Default Spreadsheet ID: {self.spreadsheet_id[:5]}...")
            
        self.sheet_name = 'Finance Bladi'
        self.headers = DataProcessor.COLUMNS
        
        if credentials_path:
            self.credentials_path = credentials_path
        else:
            self.credentials_path = os.path.join(PROJECT_ROOT, 'credentials.json')
    
    def export(self, data_row: List[List[Any]]) -> bool:
        try:
            print("\\n📤 Connecting to Google Sheets...")
            import gspread
            from google.oauth2.service_account import Credentials
            
            if not os.path.exists(self.credentials_path):
                print(f"❌ Credentials not found: {self.credentials_path}")
                return False
            
            scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            credentials = Credentials.from_service_account_file(self.credentials_path, scopes=scopes)
            client = gspread.authorize(credentials)
            
            print(f"📄 Opening spreadsheet ID: {self.spreadsheet_id}")
            spreadsheet = client.open_by_key(self.spreadsheet_id)
            print(f"✅ Spreadsheet: {spreadsheet.title}")
            
            try:
                worksheet = spreadsheet.worksheet(self.sheet_name)
            except:
                print(f"📝 Creating new sheet: '{self.sheet_name}'")
                worksheet = spreadsheet.add_worksheet(title=self.sheet_name, rows=1000, cols=len(self.headers))
            
            self._ensure_headers(worksheet)
            
            # Append data
            clean_data_row = []
            for cell in data_row[0]:
                if isinstance(cell, float) and math.isnan(cell):
                    clean_data_row.append('')
                elif cell is None:
                    clean_data_row.append('')
                else:
                    clean_data_row.append(str(cell))
            
            worksheet.append_row(clean_data_row)
            print(f"✅ Appended new row successfully")
            return True
            
        except Exception as e:
            print(f"❌ Google Sheets export failed: {e}")
            logger.error(f"Google Sheets export failed: {e}")
            return False
    
    def _ensure_headers(self, worksheet) -> bool:
        try:
            first_row = worksheet.row_values(1)
            if not first_row or first_row[0] != 'Date':
                worksheet.clear()
                worksheet.append_row(self.headers)
                return True
            return True
        except Exception:
            return False

# ============================================================================
# LOCAL DATA STORAGE
# ============================================================================

class LocalStorage:
    def __init__(self):
        self.data_dir = os.path.join(PROJECT_ROOT, 'data')
        os.makedirs(self.data_dir, exist_ok=True)
    
    def save(self, raw_data: Dict, processed_row: List[List[Any]]):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        clean_raw_data = DataCleaner.fix_nan_values(raw_data)
        
        raw_file = os.path.join(self.data_dir, f'raw_{timestamp}.json')
        with open(raw_file, 'w') as f:
            json.dump(clean_raw_data, f, indent=2, default=str)
        print(f"💾 Raw data saved: {raw_file}")
        
        csv_file = os.path.join(self.data_dir, f'data_{timestamp}.csv')
        with open(csv_file, 'w') as f:
            f.write(','.join(DataProcessor.COLUMNS) + '\\n')
            clean_row = []
            for cell in processed_row[0]:
                if isinstance(cell, float) and math.isnan(cell):
                    clean_row.append('')
                else:
                    clean_row.append(str(cell) if cell is not None else '')
            f.write(','.join(clean_row) + '\\n')
        print(f"💾 CSV data saved: {csv_file}")

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    print("\\n" + "="*60)
    print("🚀 FINANCE BLADI AUTOMATION")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\\n📍 Project root: {PROJECT_ROOT}")
    
    # Credentials logic
    creds_path = os.environ.get('GOOGLE_CREDENTIALS_PATH')
    if not creds_path:
        possible_paths = [
            'credentials.json',
            os.path.join(PROJECT_ROOT, 'credentials.json'),
            '/home/runner/work/finance-bladi-automation/finance-bladi-automation/credentials.json'
        ]
        for path in possible_paths:
            if os.path.exists(path):
                creds_path = path
                break
    
    if not creds_path or not os.path.exists(creds_path):
        print("❌ No credentials.json found!")
        # If running on GitHub Actions without credentials, we might want to exit gracefully
        return False
    
    print(f"✅ Using credentials: {creds_path}")

    # Initialize components
    collector = ModuleCollector()
    processor = DataProcessor()
    sheets_exporter = GoogleSheetsExporter(credentials_path=creds_path)
    local_storage = LocalStorage()
    
    try:
        # 1. Collect data
        print("\\n📦 Collecting data from modules...")
        raw_data = collector.collect_all()
        
        if not raw_data:
            print("❌ No data collected.")
            # Don't exit here, maybe we have partial data or want to retry
            
        # 2. Clean & Process
        raw_data = DataCleaner.clean_data_for_export(raw_data)
        print("\\n🔄 Processing data into unified format...")
        processed_data = processor.process(raw_data)
        
        # 3. Export
        print("\\n" + "="*60)
        export_success = sheets_exporter.export(processed_data)
        
        if not export_success:
            print("❌ Google Sheets export failed")
        
        # 4. Save local
        print("\\n💾 Saving local backup...")
        local_storage.save(raw_data, processed_data)
        
        return export_success
        
    except Exception as e:
        print(f"\\n❌ Critical error: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
"""
}