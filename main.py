#!/usr/bin/env python3
"""
FINANCE BLADI AUTOMATION - FIXED VERSION
Handles nan values and improves data extraction
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
# DATA CLEANER - FIXES nan VALUES
# ============================================================================

class DataCleaner:
    """Cleans and fixes data before export"""
    
    @staticmethod
    def fix_nan_values(data: Any) -> Any:
        """Replace nan values with empty strings"""
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
        """Clean entire dataset for Google Sheets"""
        cleaned = {}
        for key, value in raw_data.items():
            cleaned[key] = DataCleaner.fix_nan_values(value)
        return cleaned

# ============================================================================
# MODULE COLLECTOR - IMPROVED
# ============================================================================

class ModuleCollector:
    """Collects data from all financial modules"""
    
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
        """Collect data from all modules"""
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
        """Collect data from a single module"""
        try:
            module = __import__(module_name)
            
            # Try different function names
            if hasattr(module, 'collect_data'):
                return module.collect_data()
            elif hasattr(module, 'main'):
                return module.main()
            elif hasattr(module, 'run'):
                return module.run()
            elif hasattr(module, 'get_data'):
                return module.get_data()
            else:
                # Find any callable that looks like a main function
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
# DATA PROCESSOR - IMPROVED EXTRACTION
# ============================================================================

class DataProcessor:
    """Processes raw data into unified format for Google Sheets"""
    
    # Define the exact column order for Google Sheets
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
        """Extract value from nested dicts using dot notation paths"""
        if not isinstance(data, dict):
            return ''
        
        for path in paths:
            try:
                # Split path by dots for nested access
                keys = path.split('.')
                current = data
                
                for key in keys:
                    if isinstance(current, dict) and key in current:
                        current = current[key]
                    else:
                        current = None
                        break
                
                if current is not None and current != '':
                    # Clean the value
                    if isinstance(current, float) and math.isnan(current):
                        return ''
                    return current
                    
            except (KeyError, AttributeError, TypeError):
                continue
        
        return ''
    
    @staticmethod
    def _clean_masi_value(value: Any) -> Any:
        """Clean MASI value (remove commas from '19,445.46')"""
        if isinstance(value, str):
            # Remove commas from numbers like "19,445.46"
            return value.replace(',', '')
        return value
    
    def process(self, raw_data: Dict[str, Any]) -> List[List[Any]]:
        """Convert raw module data to unified row format"""
        today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row = [today]
        
        # Clean the data first
        raw_data = DataCleaner.fix_nan_values(raw_data)
        
        # 1. EUR/MAD
        eur_mad = self._extract_nested_value(
            raw_data.get('bkam_forex', {}),
            'EUR/MAD', 'eur_mad', 'EUR_MAD'
        )
        row.append(eur_mad if eur_mad not in ['', None] else '')
        
        # 2. USD/MAD
        usd_mad = self._extract_nested_value(
            raw_data.get('bkam_forex', {}),
            'USD/MAD', 'usd_mad', 'USD_MAD'
        )
        row.append(usd_mad if usd_mad not in ['', None] else '')
        
        # 3-5. Treasury rates
        treasury_data = raw_data.get('bkam_treasury', {})
        row.append(self._extract_nested_value(treasury_data, 'BT2Y', 'bt2y', '2Y'))
        row.append(self._extract_nested_value(treasury_data, 'BT5Y', 'bt5y', '5Y'))
        row.append(self._extract_nested_value(treasury_data, 'BT10Y', 'bt10y', '10Y'))
        
        # 6. MASI
        masi_value = self._extract_nested_value(
            raw_data.get('investing_masi', {}),
            'MASI', 'masi', 'value'
        )
        row.append(self._clean_masi_value(masi_value) if masi_value not in ['', None] else '')
        
        # 7. Phosphate DAP
        phosphate_value = self._extract_nested_value(
            raw_data.get('trading_economics', {}),
            'Phosphate DAP', 'phosphate', 'DAP', 'value', 'PHOSPHATE_DAP'
        )
        row.append(phosphate_value if phosphate_value not in ['', None] else '')
        
        # 8-20. Yahoo Finance data
        yahoo_data = raw_data.get('yahoo_markets', {})
        
        # Extract Yahoo values with better fallbacks
        yahoo_extractors = [
            ('BRENT', ['BRENT', 'brent']),
            ('WTI', ['WTI', 'wti']),
            ('GOLD', ['GOLD', 'gold', 'XAU']),
            ('SILVER', ['SILVER', 'silver', 'XAG']),
            ('BITCOIN', ['BITCOIN', 'bitcoin', 'BTC']),
            ('EURUSD', ['EURUSD', 'eurusd', 'EUR/USD']),
            ('USDJPY', ['USDJPY', 'usdjpy', 'USD/JPY']),
            ('GBPUSD', ['GBPUSD', 'gbpusd', 'GBP/USD']),
            ('SP500', ['SP500', 'sp500', 'S&P500', '^GSPC']),
            ('DJIA', ['DJIA', 'dow', 'DOW', '^DJI']),
            ('NASDAQ', ['NASDAQ', 'nasdaq', '^IXIC', 'NAS100']),
            ('US10Y', ['US10Y', 'us10y', '10Y', 'UST10Y']),
            ('VIX', ['VIX', 'vix', '^VIX'])
        ]
        
        for _, keys in yahoo_extractors:
            value = self._extract_nested_value(yahoo_data, *keys)
            # Ensure nan values are empty strings
            if isinstance(value, float) and math.isnan(value):
                row.append('')
            else:
                row.append(value if value not in ['', None] else '')
        
        # Clean the entire row
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
# GOOGLE SHEETS EXPORTER - FIXED FOR nan
# ============================================================================

class GoogleSheetsExporter:
    """Exports data to Google Sheets - FIXED for nan values"""
    
    def __init__(self):
        self.spreadsheet_id = '1unwXUkxs7boI1I29iumlJd3E9WcK9BngF_D4NFWDb90'
        self.sheet_name = 'Finance Bladi'
        self.headers = DataProcessor.COLUMNS
    
    def export(self, data_row: List[List[Any]]) -> bool:
        """Export data to Google Sheets - FIXED version"""
        try:
            print("\n📤 Connecting to Google Sheets...")
            
            import gspread
            from google.oauth2.service_account import Credentials
            import json as json_lib
            
            # Check credentials
            creds_path = os.path.join(PROJECT_ROOT, 'credentials.json')
            if not os.path.exists(creds_path):
                print(f"❌ Credentials not found: {creds_path}")
                return False
            
            print(f"✅ Using credentials: {creds_path}")
            
            # Authenticate
            scopes = ['https://www.googleapis.com/auth/spreadsheets',
                     'https://www.googleapis.com/auth/drive']
            credentials = Credentials.from_service_account_file(creds_path, scopes=scopes)
            client = gspread.authorize(credentials)
            
            # Open spreadsheet
            print(f"📄 Opening spreadsheet: {self.spreadsheet_id}")
            spreadsheet = client.open_by_key(self.spreadsheet_id)
            print(f"✅ Spreadsheet: {spreadsheet.title}")
            
            # Get or create worksheet
            try:
                worksheet = spreadsheet.worksheet(self.sheet_name)
                print(f"📋 Using existing sheet: '{self.sheet_name}'")
            except:
                print(f"📝 Creating new sheet: '{self.sheet_name}'")
                worksheet = spreadsheet.add_worksheet(
                    title=self.sheet_name,
                    rows=1000,
                    cols=len(self.headers)
                )
            
            # ENSURE HEADERS EXIST
            self._ensure_headers(worksheet)
            
            # Get all existing data
            all_data = worksheet.get_all_values()
            
            # Check if today already exists
            today_date = datetime.now().strftime('%Y-%m-%d')
            today_exists = False
            existing_row = None
            
            for i, row in enumerate(all_data[1:], start=2):
                if row and row[0]:
                    try:
                        row_date = row[0].split()[0]
                        if row_date == today_date:
                            today_exists = True
                            existing_row = i
                            break
                    except:
                        continue
            
            # Prepare data for export - ensure no nan values
            clean_data_row = []
            for cell in data_row[0]:
                if isinstance(cell, float) and math.isnan(cell):
                    clean_data_row.append('')
                elif cell is None:
                    clean_data_row.append('')
                else:
                    clean_data_row.append(str(cell))
            
            if today_exists and existing_row:
                # Update existing row
                print(f"📝 Updating existing row for {today_date}...")
                for col_idx, value in enumerate(clean_data_row, start=1):
                    worksheet.update_cell(existing_row, col_idx, value)
                print(f"✅ Updated row {existing_row}")
            else:
                # Append new row
                print(f"📝 Adding new row for {today_date}...")
                try:
                    worksheet.append_row(clean_data_row)
                    print(f"✅ Appended new row")
                except Exception as append_error:
                    print(f"⚠️ Append failed, trying cell-by-cell update...")
                    # Fallback to cell-by-cell update
                    next_row = len(all_data) + 1
                    for col_idx, value in enumerate(clean_data_row, start=1):
                        worksheet.update_cell(next_row, col_idx, value)
                    print(f"✅ Added row {next_row} cell-by-cell")
            
            print("🎯 Google Sheets export completed successfully")
            return True
            
        except Exception as e:
            print(f"❌ Google Sheets export failed: {e}")
            logger.error(f"Google Sheets export failed: {e}")
            return False
    
    def _ensure_headers(self, worksheet) -> bool:
        """Make sure headers exist in first row"""
        try:
            first_row = worksheet.row_values(1)
            
            if not first_row or first_row[0] != 'Date':
                print("📋 Creating headers...")
                worksheet.clear()
                worksheet.append_row(self.headers)
                print(f"✅ Created {len(self.headers)} headers")
                return True
            else:
                print("✅ Headers already exist")
                return True
        except Exception as e:
            print(f"❌ Error with headers: {e}")
            return False

# ============================================================================
# LOCAL DATA STORAGE
# ============================================================================

class LocalStorage:
    """Saves data locally as backup"""
    
    def __init__(self):
        self.data_dir = os.path.join(PROJECT_ROOT, 'data')
        os.makedirs(self.data_dir, exist_ok=True)
    
    def save(self, raw_data: Dict, processed_row: List[List[Any]]):
        """Save data to local files"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Clean raw_data for JSON serialization
        clean_raw_data = DataCleaner.fix_nan_values(raw_data)
        
        # Save raw data as JSON
        raw_file = os.path.join(self.data_dir, f'raw_{timestamp}.json')
        with open(raw_file, 'w') as f:
            json.dump(clean_raw_data, f, indent=2, default=str)
        print(f"💾 Raw data saved: {raw_file}")
        
        # Save processed data as CSV
        csv_file = os.path.join(self.data_dir, f'data_{timestamp}.csv')
        with open(csv_file, 'w') as f:
            # Write headers
            f.write(','.join(DataProcessor.COLUMNS) + '\n')
            # Write data
            clean_row = []
            for cell in processed_row[0]:
                if isinstance(cell, float) and math.isnan(cell):
                    clean_row.append('')
                else:
                    clean_row.append(str(cell) if cell is not None else '')
            f.write(','.join(clean_row) + '\n')
        print(f"💾 CSV data saved: {csv_file}")

# ============================================================================
# MAIN APPLICATION
# ============================================================================
# Add this near the top of your main() function
def safe_collect_data():
    """Collect data with error handling"""
    results = {}
    
    # Try each module with timeout
    modules = [
        ('bkam_forex', 'collect_forex_data'),
        ('bkam_treasury', 'collect_treasury_data'),
        ('investing_masi', 'collect_masi_data'),
        ('trading_economics', 'collect_phosphate_data'),
        ('yahoo_markets', 'collect_market_data')
    ]
    
    for module_name, func_name in modules:
        try:
            print(f"📦 {module_name}...", end='')
            
            # Import and run
            module = __import__(module_name)
            func = getattr(module, func_name, None) or getattr(module, 'collect_data', None)
            
            if func:
                data = func()
                if data:
                    results[module_name] = data
                    print("✅")
                else:
                    print("⚠️ No data")
            else:
                print("❌ No function")
                
        except Exception as e:
            print(f"❌ Error: {str(e)[:50]}")
            # Use fallback data
            results[module_name] = get_fallback_data(module_name)
    
    return results

def get_fallback_data(module_name):
    """Provide fallback data when module fails"""
    fallbacks = {
        'bkam_forex': {'EUR/MAD': 'N/A', 'USD/MAD': 'N/A'},
        'bkam_treasury': {'BT2Y': 'N/A', 'BT5Y': 'N/A', 'BT10Y': 'N/A'},
        'investing_masi': {'MASI': 'N/A'},
        'trading_economics': {'Phosphate DAP': 'N/A'}
    }
    return fallbacks.get(module_name, {'error': 'Module failed'})
def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("🚀 FINANCE BLADI AUTOMATION")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Initialize components
    collector = ModuleCollector()
    processor = DataProcessor()
    sheets_exporter = GoogleSheetsExporter()
    local_storage = LocalStorage()
    
    try:
        # 1. Collect data from all modules
        print("📦 Collecting data from modules...")
        raw_data = collector.collect_all()
        
        if not raw_data:
            print("❌ No data collected. Exiting.")
            return False
        
        print(f"✅ Collected data from {len(raw_data)} modules")
        
        # 2. Clean the raw data
        raw_data = DataCleaner.clean_data_for_export(raw_data)
        
        # 3. Process data into unified format
        print("\n🔄 Processing data into unified format...")
        processed_data = processor.process(raw_data)
        
        # Show what we collected
        print("\n📊 KEY DATA COLLECTED:")
        key_data = {
            'EUR/MAD': None,
            'USD/MAD': None,
            'BT2Y': None,
            'MASI': None,
            'Phosphate DAP': None,
            'BITCOIN': None
        }
        
        # Extract key values
        forex_data = raw_data.get('bkam_forex', {})
        if isinstance(forex_data, dict):
            key_data['EUR/MAD'] = forex_data.get('EUR/MAD')
            key_data['USD/MAD'] = forex_data.get('USD/MAD')
        
        treasury_data = raw_data.get('bkam_treasury', {})
        if isinstance(treasury_data, dict):
            key_data['BT2Y'] = treasury_data.get('BT2Y')
        
        masi_data = raw_data.get('investing_masi', {})
        if isinstance(masi_data, dict):
            key_data['MASI'] = masi_data.get('MASI')
        
        phosphate_data = raw_data.get('trading_economics', {})
        if isinstance(phosphate_data, dict):
            key_data['Phosphate DAP'] = phosphate_data.get('Phosphate DAP')
        
        yahoo_data = raw_data.get('yahoo_markets', {})
        if isinstance(yahoo_data, dict):
            key_data['BITCOIN'] = yahoo_data.get('BITCOIN')
        
        for key, value in key_data.items():
            if value is not None and value != '':
                print(f"  • {key}: {value}")
        
        # 4. Export to Google Sheets
        print("\n" + "="*60)
        export_success = sheets_exporter.export(processed_data)
        
        if export_success:
            print("\n✅ Data organized in Google Sheets:")
            print(f"   • Sheet: '{sheets_exporter.sheet_name}'")
            print(f"   • Columns: {len(sheets_exporter.headers)}")
            print(f"   • URL: https://docs.google.com/spreadsheets/d/{sheets_exporter.spreadsheet_id}")
            print(f"   • Today's row added: {datetime.now().strftime('%Y-%m-%d')}")
        else:
            print("\n❌ Google Sheets export failed")
        
        # 5. Save local backup
        print("\n💾 Saving local backup...")
        local_storage.save(raw_data, processed_data)
        
        # 6. Summary
        print("\n" + "="*60)
        print("🎯 EXECUTION SUMMARY")
        print("="*60)
        print(f"Modules: {len(raw_data)}/{len(collector.MODULES)} successful")
        print(f"Errors: {len(collector.errors)}")
        
        if collector.errors:
            print("\n⚠️ Errors encountered:")
            for error in collector.errors[:3]:
                print(f"  • {error}")
        
        print(f"\n📁 Local data: {local_storage.data_dir}/")
        print(f"📋 Logs: {os.path.join(PROJECT_ROOT, 'logs')}/")
        
        return export_success
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        return False
    except Exception as e:
        print(f"\n❌ Critical error: {e}")
        logger.error(f"Critical error: {e}")
        traceback.print_exc()
        return False

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Save this as main.py in your project root
    success = main()
    
    if success:
        print("\n✅ FINANCE BLADI AUTOMATION COMPLETED SUCCESSFULLY")
    else:
        print("\n❌ FINANCE BLADI AUTOMATION FAILED")
    
    sys.exit(0 if success else 1)