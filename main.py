import os
import sys
import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials
from modules.bkam_forex import collect_bkam_forex
from modules.bkam_treasury import collect_bkam_treasury
from modules.investing_masi import collect_investing_masi
from modules.trading_economics import collect_trading_economics
from modules.yahoo_markets import collect_yahoo_markets

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/finance_bladi_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FinanceBladiAutomation:
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.data_dir = self.project_root / "data"
        self.logs_dir = self.project_root / "logs"
        self.data_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        
        # Your spreadsheet ID
        self.spreadsheet_id = '1unwXUkxs7boI1I29iumlJd3E9WcK9BngF_D4NFWDb90'
        
        # Column mapping for Google Sheets
        self.column_mapping = {
            'EUR/MAD': 'B',
            'USD/MAD': 'C',
            'BT2Y': 'D',
            'BT5Y': 'E',
            'BT10Y': 'F',
            'MASI': 'G',
            'PHOSPHATE_DAP': 'H',
            'BRENT': 'I',
            'WTI': 'J',
            'GAS': 'K',
            'GOLD': 'L',
            'SILVER': 'M',
            'COPPER': 'N',
            'SP500': 'O',
            'DJIA': 'P',
            'NASDAQ': 'Q',
            'RUSSELL2000': 'R',
            'CAC40': 'S',
            'DAX': 'T',
            'FTSE100': 'U',
            'US10Y': 'V',
            'VIX': 'W',
            'BITCOIN': 'X',
            'EURUSD': 'Y',
            'USDJPY': 'Z',
            'GBPUSD': 'AA',
            'USDCAD': 'AB',
            'AUDUSD': 'AC'
        }
        
        self.all_data = {}
        
    def collect_data(self):
        """Collect data from all modules"""
        logger.info("=" * 60)
        logger.info("STARTING DATA COLLECTION")
        logger.info("=" * 60)
        
        modules = [
            ("bkam_forex", collect_bkam_forex),
            ("bkam_treasury", collect_bkam_treasury),
            ("investing_masi", collect_investing_masi),
            ("trading_economics", collect_trading_economics),
            ("yahoo_markets", collect_yahoo_markets)
        ]
        
        successful = 0
        for name, module_func in modules:
            logger.info(f"Collecting {name}...")
            try:
                data = module_func()
                if data:
                    self.all_data.update(data)
                    logger.info(f"✅ {name}: Success")
                    successful += 1
                else:
                    logger.warning(f"⚠️ {name}: No data returned")
            except Exception as e:
                logger.error(f"❌ {name}: {str(e)}")
        
        logger.info(f"Collection complete: {successful}/{len(modules)} modules succeeded")
        return successful > 0
    
    def process_data(self):
        """Process and unify all collected data"""
        logger.info("Processing data into unified format...")
        
        # Ensure all required keys exist
        processed_data = {
            'timestamp': datetime.now().isoformat(),
            'date': datetime.now().strftime('%Y-%m-%d')
        }
        
        # Add all collected data
        for key, value in self.all_data.items():
            if isinstance(value, (int, float, str)):
                processed_data[key] = value
            elif value is not None:
                processed_data[key] = str(value)
        
        # Log key metrics
        logger.info("\n📊 KEY DATA COLLECTED:")
        for key in ['EUR/MAD', 'USD/MAD', 'BT2Y', 'MASI', 'BITCOIN']:
            if key in processed_data:
                logger.info(f"  • {key}: {processed_data[key]}")
        
        logger.info("=" * 60)
        return processed_data
    
    def connect_to_google_sheets(self):
        """Connect to Google Sheets API"""
        logger.info("\n📤 Connecting to Google Sheets...")
        
        # Find credentials
        credentials_path = self.project_root / "credentials.json"
        if not credentials_path.exists():
            logger.error(f"❌ Credentials file not found at: {credentials_path}")
            return None
        
        try:
            # Authenticate
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            credentials = Credentials.from_service_account_file(
                str(credentials_path),
                scopes=scopes
            )
            client = gspread.authorize(credentials)
            
            logger.info(f"✅ Credentials loaded from: {credentials_path}")
            logger.info(f"📊 Opening spreadsheet ID: {self.spreadsheet_id}")
            
            # Open spreadsheet
            spreadsheet = client.open_by_key(self.spreadsheet_id)
            logger.info(f"✅ Spreadsheet found: {spreadsheet.title}")
            
            return spreadsheet
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"❌ Spreadsheet not found. Check if ID '{self.spreadsheet_id}' is correct")
            logger.error("📋 Make sure the spreadsheet is shared with the service account:")
            logger.error("   928771074788-compute@developer.gserviceaccount.com")
            return None
        except Exception as e:
            logger.error(f"❌ Google Sheets connection failed: {str(e)}")
            return None
    
    def update_google_sheets(self, data):
        """Update Google Sheets with collected data"""
        spreadsheet = self.connect_to_google_sheets()
        if not spreadsheet:
            return False
        
        try:
            # Get or create worksheet
            try:
                worksheet = spreadsheet.worksheet("Finance Data")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet("Finance Data", rows=1000, cols=50)
                logger.info("📄 Created new worksheet: Finance Data")
            
            # Find next empty row
            all_values = worksheet.get_all_values()
            next_row = len(all_values) + 1
            
            # Prepare row data
            row_data = []
            for key, col_letter in self.column_mapping.items():
                value = data.get(key, '')
                row_data.append(value)
            
            # Add timestamp
            row_data.insert(0, data['timestamp'])
            
            # Update the row
            worksheet.update(f'A{next_row}', [row_data])
            
            logger.info(f"✅ Data written to row {next_row}")
            
            # Update headers if first row
            if next_row == 2:  # First data row
                headers = ['Timestamp'] + list(self.column_mapping.keys())
                worksheet.update('A1', [headers])
                logger.info("📋 Headers updated")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update Google Sheets: {str(e)}")
            return False
    
    def save_local_backup(self, data):
        """Save data locally as backup"""
        try:
            # Save as JSON
            json_path = self.data_dir / f"raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Raw data saved: {json_path}")
            
            # Save as CSV
            df = pd.DataFrame([data])
            csv_path = self.data_dir / f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(csv_path, index=False, encoding='utf-8')
            logger.info(f"💾 CSV data saved: {csv_path}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save local backup: {str(e)}")
            return False
    
    def run(self):
        """Main execution function"""
        logger.info("=" * 60)
        logger.info("🚀 FINANCE BLADI AUTOMATION")
        logger.info("=" * 60)
        logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📍 Project root: {self.project_root}")
        
        try:
            # Step 1: Collect data
            if not self.collect_data():
                logger.error("❌ Data collection failed")
                return False
            
            # Step 2: Process data
            processed_data = self.process_data()
            
            # Step 3: Save local backup
            self.save_local_backup(processed_data)
            
            # Step 4: Update Google Sheets
            sheets_success = self.update_google_sheets(processed_data)
            
            # Step 5: Summary
            logger.info("=" * 60)
            logger.info("🎯 EXECUTION SUMMARY")
            logger.info("=" * 60)
            logger.info("✅ Data collection completed")
            logger.info("✅ Data processed and saved locally")
            
            if sheets_success:
                logger.info("✅ Data exported to Google Sheets")
                logger.info("✨ FINANCE BLADI AUTOMATION COMPLETED SUCCESSFULLY")
            else:
                logger.warning("⚠️ Google Sheets export failed, but data is saved locally")
                logger.info("📁 Local data available in: data/")
                logger.info("🔧 Fix Google Sheets configuration for automatic export")
            
            logger.info(f"📍 Data directory: {self.data_dir}")
            logger.info(f"📍 Logs directory: {self.logs_dir}")
            logger.info("=" * 60)
            
            return sheets_success
            
        except Exception as e:
            logger.error(f"❌ Fatal error: {str(e)}", exc_info=True)
            return False

def main():
    """Main entry point"""
    automation = FinanceBladiAutomation()
    
    try:
        success = automation.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("⚠️ Process interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()