# utils/google_sheets.py - FIXED HEADERS VERSION
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from typing import Dict, Any, List
import logging
import traceback
import json
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class UnifiedDataExporter:
    """Exporter that organizes all data in a single unified format with proper headers"""
    
    def __init__(self, credentials_path: str):
        """Initialize Google Sheets exporter"""
        self.credentials_path = credentials_path
        self.client = None
        self.spreadsheet_id = '1unwXUkxs7boI1I29iumlJd3E9WcK9BngF_D4NFWDb90'
        
        # Standard columns in order
        self.standard_columns = [
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
    
    def _authenticate(self):
        """Authenticate with Google Sheets API"""
        try:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            credentials = Credentials.from_service_account_file(
                self.credentials_path, scopes=scopes
            )
            self.client = gspread.authorize(credentials)
            logger.info("Google Sheets authentication successful")
            return True
        except Exception as e:
            logger.error(f"Google Sheets authentication failed: {str(e)}")
            return False
    
    def _ensure_headers_exist(self, worksheet) -> bool:
        """Make sure headers exist in the first row"""
        try:
            # Get first row
            first_row = worksheet.row_values(1)
            
            # If first row is empty or doesn't match our standard headers, recreate them
            if not first_row or first_row[0] != 'Date':
                print("📋 Creating/refreshing headers...")
                
                # Clear the entire sheet
                worksheet.clear()
                
                # Add headers as first row
                worksheet.append_row(self.standard_columns)
                
                print(f"✅ Created headers: {self.standard_columns}")
                return True
            else:
                print(f"✅ Headers already exist: {first_row[:5]}...")
                return True
                
        except Exception as e:
            print(f"❌ Error ensuring headers: {e}")
            return False
    
    def _extract_data_values(self, all_data: Dict[str, Any]) -> List:
        """Extract values from all_data into the standard column order"""
        today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = [today]  # Start with date
        
        # Helper function to safely extract values
        def get_value(data_dict, possible_keys, default=''):
            if not isinstance(data_dict, dict):
                return default
            
            for key in possible_keys:
                if key in data_dict:
                    val = data_dict[key]
                    # Handle nested values
                    if isinstance(val, dict):
                        return val.get('value', val.get('data', default))
                    return val
            return default
        
        # Extract each value in the correct order
        # 1. EUR/MAD
        values.append(get_value(all_data.get('bkam_forex', {}), 
                              ['EUR/MAD', 'eur_mad', 'EUR_MAD', 'EUR_MAD_value']))
        
        # 2. USD/MAD
        values.append(get_value(all_data.get('bkam_forex', {}), 
                              ['USD/MAD', 'usd_mad', 'USD_MAD', 'USD_MAD_value']))
        
        # 3. BT2Y (%)
        values.append(get_value(all_data.get('bkam_treasury', {}), 
                              ['BT2Y', 'bt2y', '2Y', '2y', 'BT2Y_value']))
        
        # 4. BT5Y (%)
        values.append(get_value(all_data.get('bkam_treasury', {}), 
                              ['BT5Y', 'bt5y', '5Y', '5y', 'BT5Y_value']))
        
        # 5. BT10Y (%)
        values.append(get_value(all_data.get('bkam_treasury', {}), 
                              ['BT10Y', 'bt10y', '10Y', '10y', 'BT10Y_value']))
        
        # 6. MASI
        values.append(get_value(all_data.get('investing_masi', {}), 
                              ['MASI', 'masi', 'value', 'index']))
        
        # 7. Phosphate DAP (USD/T)
        values.append(get_value(all_data.get('trading_economics', {}), 
                              ['Phosphate DAP', 'phosphate', 'DAP', 'value', 'price']))
        
        # Yahoo Finance Data (8-20)
        yahoo_data = all_data.get('yahoo_markets', {})
        
        # 8. BRENT (USD)
        values.append(get_value(yahoo_data, ['BRENT', 'brent', 'OIL_BRENT']))
        
        # 9. WTI (USD)
        values.append(get_value(yahoo_data, ['WTI', 'wti', 'OIL_WTI']))
        
        # 10. GOLD (USD)
        values.append(get_value(yahoo_data, ['GOLD', 'gold', 'XAU']))
        
        # 11. SILVER (USD)
        values.append(get_value(yahoo_data, ['SILVER', 'silver', 'XAG']))
        
        # 12. BITCOIN (USD)
        values.append(get_value(yahoo_data, ['BITCOIN', 'bitcoin', 'BTC']))
        
        # 13. EUR/USD
        values.append(get_value(yahoo_data, ['EURUSD', 'eurusd', 'EUR_USD']))
        
        # 14. USD/JPY
        values.append(get_value(yahoo_data, ['USDJPY', 'usdjpy', 'USD_JPY']))
        
        # 15. GBP/USD
        values.append(get_value(yahoo_data, ['GBPUSD', 'gbpusd', 'GBP_USD']))
        
        # 16. S&P 500
        values.append(get_value(yahoo_data, ['SP500', 'sp500', 'S&P500', '^GSPC']))
        
        # 17. Dow Jones
        values.append(get_value(yahoo_data, ['DJIA', 'dow', 'DOW', '^DJI']))
        
        # 18. NASDAQ
        values.append(get_value(yahoo_data, ['NASDAQ', 'nasdaq', '^IXIC', 'NAS100']))
        
        # 19. US 10Y Yield (%)
        values.append(get_value(yahoo_data, ['US10Y', 'us10y', '10Y', 'UST10Y']))
        
        # 20. VIX
        values.append(get_value(yahoo_data, ['VIX', 'vix', '^VIX']))
        
        return values
    
    def export_unified_data(self, all_data: Dict[str, Any]) -> bool:
        """Export all data to a single unified sheet with proper headers"""
        try:
            print("\n" + "="*60)
            print("📤 EXPORTING DATA TO GOOGLE SHEETS")
            print("="*60)
            
            # Authenticate
            if not self._authenticate():
                print("❌ Authentication failed")
                return False
            
            # Open spreadsheet
            try:
                spreadsheet = self.client.open_by_key(self.spreadsheet_id)
                print(f"✅ Opened spreadsheet: {spreadsheet.title}")
            except Exception as e:
                print(f"❌ Error opening spreadsheet: {e}")
                return False
            
            # Use "Finance Bladi" as sheet name
            sheet_name = "Finance Bladi"
            
            # Get or create worksheet
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                print(f"✅ Using existing worksheet: '{sheet_name}'")
            except:
                print(f"📝 Creating new worksheet: '{sheet_name}'")
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name, 
                    rows=1000, 
                    cols=len(self.standard_columns)
                )
            
            # ENSURE HEADERS EXIST
            if not self._ensure_headers_exist(worksheet):
                print("❌ Failed to ensure headers exist")
                return False
            
            # Extract today's data
            today_values = self._extract_data_values(all_data)
            
            # Get all existing rows
            all_rows = worksheet.get_all_values()
            
            # Check if we have only headers (no data yet)
            if len(all_rows) <= 1:
                # First data entry - add to row 2
                print("📊 Adding first data row...")
                worksheet.append_row(today_values)
                print(f"✅ Added first data row with {len(today_values)} values")
                return True
            
            # Check if today's date already exists
            today_date = datetime.now().strftime("%Y-%m-%d")
            existing_dates = []
            
            # Skip header row (row 1)
            for i, row in enumerate(all_rows[1:], start=2):
                if row and row[0]:  # Check if row has data and date exists
                    # Extract date part from timestamp
                    try:
                        row_date = row[0].split()[0]  # Get YYYY-MM-DD part
                        existing_dates.append((i, row_date))
                    except:
                        continue
            
            # Check if today exists
            today_exists = False
            existing_row_index = None
            
            for row_index, row_date in existing_dates:
                if row_date == today_date:
                    today_exists = True
                    existing_row_index = row_index
                    break
            
            if today_exists and existing_row_index:
                # Update existing row
                print(f"📝 Updating existing row for {today_date} (row {existing_row_index})...")
                
                # Update each cell
                for col_index, value in enumerate(today_values, start=1):
                    worksheet.update_cell(existing_row_index, col_index, value)
                
                print(f"✅ Updated row {existing_row_index}")
            else:
                # Add new row
                print(f"📝 Adding new row for {today_date}...")
                worksheet.append_row(today_values)
                print(f"✅ Added new row (row {len(all_rows) + 1})")
            
            print("\n" + "="*60)
            print("✅ EXPORT COMPLETED SUCCESSFULLY")
            print("="*60)
            
            # Show preview
            print(f"\n📋 Data Preview:")
            print(f"   Date: {today_values[0]}")
            print(f"   EUR/MAD: {today_values[1]}")
            print(f"   USD/MAD: {today_values[2]}")
            print(f"   MASI: {today_values[6]}")
            print(f"   ... and {len(today_values)-7} more values")
            
            return True
            
        except Exception as e:
            print(f"\n❌ Export failed: {str(e)}")
            logger.error(f"Google Sheets export failed: {str(e)}")
            logger.debug(traceback.format_exc())
            return False

def test_with_real_data():
    """Test with real data structure from your modules"""
    print("🧪 Testing with sample data...")
    
    # Sample data matching your actual modules
    sample_data = {
        'bkam_forex': {'EUR/MAD': 10.7496, 'USD/MAD': 9.172},
        'bkam_treasury': {'BT2Y': 2.495, 'BT5Y': 2.658, 'BT10Y': 2.982},
        'investing_masi': {'MASI': 19445.46},
        'trading_economics': {'Phosphate DAP': 625.0},
        'yahoo_markets': {
            'BRENT': 60.36,
            'WTI': 56.44,
            'GOLD': 4471.5,
            'SILVER': 78.44,
            'BITCOIN': 91291.41,
            'EURUSD': 1.168,
            'USDJPY': 156.74,
            'GBPUSD': 1.3458,
            'SP500': 6920.93,
            'DJIA': 48996.08,
            'NASDAQ': 25653.90,
            'US10Y': 4.138,
            'VIX': 15.38
        }
    }
    
    exporter = UnifiedDataExporter('credentials.json')
    success = exporter.export_unified_data(sample_data)
    
    if success:
        print("\n🎯 TEST PASSED!")
        print(f"Check your Google Sheet: https://docs.google.com/spreadsheets/d/{exporter.spreadsheet_id}")
    else:
        print("\n💥 TEST FAILED")
    
    return success

# Force recreate headers if needed
def force_recreate_headers():
    """Force recreate headers in the sheet"""
    print("🔄 FORCE RECREATING HEADERS...")
    
    exporter = UnifiedDataExporter('credentials.json')
    
    if not exporter._authenticate():
        return False
    
    try:
        spreadsheet = exporter.client.open_by_key(exporter.spreadsheet_id)
        worksheet = spreadsheet.worksheet("Finance Bladi")
        
        # Clear everything
        worksheet.clear()
        
        # Add headers
        worksheet.append_row(exporter.standard_columns)
        
        print("✅ Headers recreated successfully!")
        print(f"Headers: {exporter.standard_columns}")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    # Uncomment one of these:
    
    # Test with sample data
    test_with_real_data()
    
    # OR force recreate headers
    # force_recreate_headers()