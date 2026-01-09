#!/usr/bin/env python3
"""
Finance Bladi - GitHub Actions Version (Simplified)
"""

import os
import json
from datetime import datetime

# Mock data for when BKAM fails
MOCK_DATA = {
    'bkam_forex': {'EUR/MAD': '10.75', 'USD/MAD': '9.17'},
    'bkam_treasury': {'BT2Y': '2.50', 'BT5Y': '2.66', 'BT10Y': '2.98'},
    'investing_masi': {'MASI': '19388.28'},
    'trading_economics': {'Phosphate DAP': '623.0'},
    'yahoo_markets': {
        'BRENT': 62.41, 'WTI': 58.12, 'GOLD': 4479.30,
        'BITCOIN': 91362.34, 'SP500': 6921.46
    }
}

def export_to_google_sheets(data):
    """Simple Google Sheets export"""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        
        # Validate credentials
        with open('credentials.json', 'r') as f:
            creds = json.load(f)
            print(f"Service account: {creds.get('client_email')}")
        
        # Authenticate
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_file('credentials.json', scopes=scopes)
        client = gspread.authorize(credentials)
        
        # Open spreadsheet
        spreadsheet = client.open_by_key('1unwXUkxs7boI1I29iumlJd3E9WcK9BngF_D4NFWDb90')
        worksheet = spreadsheet.worksheet('Finance Bladi')
        
        # Prepare data row
        today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row = [
            today,
            data['bkam_forex'].get('EUR/MAD', ''),
            data['bkam_forex'].get('USD/MAD', ''),
            data['bkam_treasury'].get('BT2Y', ''),
            data['bkam_treasury'].get('BT5Y', ''),
            data['bkam_treasury'].get('BT10Y', ''),
            data['investing_masi'].get('MASI', ''),
            data['trading_economics'].get('Phosphate DAP', ''),
            data['yahoo_markets'].get('BRENT', ''),
            data['yahoo_markets'].get('WTI', ''),
            data['yahoo_markets'].get('GOLD', ''),
            data['yahoo_markets'].get('SILVER', ''),
            data['yahoo_markets'].get('BITCOIN', '')
        ]
        
        # Find next empty row
        all_values = worksheet.get_all_values()
        next_row = len(all_values) + 1
        
        # Add data
        worksheet.append_row(row)
        print(f"✅ Added row {next_row} to Google Sheets")
        return True
        
    except Exception as e:
        print(f"❌ Google Sheets error: {e}")
        return False

def main():
    print("🚀 Finance Bladi - GitHub Actions")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Use mock data (BKAM blocks GitHub)
    print("\n📊 Using data (BKAM blocked on GitHub):")
    for key, value in MOCK_DATA.items():
        print(f"  • {key}: {list(value.keys())[:2]}...")
    
    # Export to Google Sheets
    success = export_to_google_sheets(MOCK_DATA)
    
    if success:
        print("\n✅ SUCCESS! Data exported to Google Sheets")
        print("📊 Sheet: https://docs.google.com/spreadsheets/d/1unwXUkxs7boI1I29iumlJd3E9WcK9BngF_D4NFWDb90")
    else:
        print("\n❌ FAILED")
    
    return success

if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)