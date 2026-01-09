# utils/google_sheets.py - Enhanced version
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from typing import Dict, Any
import logging
import traceback

logger = logging.getLogger(__name__)

class GoogleSheetsExporter:
    def __init__(self, credentials_path: str):
        """Initialize Google Sheets exporter"""
        self.credentials_path = credentials_path
        self.client = None
        self.spreadsheet_id = '1unwXUkxs7boI1I29iumlJd3E9WcK9BngF_D4NFWDb90'  # Your spreadsheet ID
        
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
        except FileNotFoundError:
            logger.error(f"Credentials file not found: {self.credentials_path}")
            return False
        except Exception as e:
            logger.error(f"Google Sheets authentication failed: {str(e)}")
            logger.debug(traceback.format_exc())
            return False
    
    def _prepare_data_for_sheets(self, data: Any) -> list:
        """Convert any data type to list of lists for Google Sheets"""
        try:
            if isinstance(data, pd.DataFrame):
                # DataFrames: include headers
                return [data.columns.tolist()] + data.values.tolist()
            elif isinstance(data, dict):
                # Dicts: convert to key-value pairs
                return [[key, str(value)] for key, value in data.items()]
            elif isinstance(data, list):
                if not data:
                    return [["No data"]]
                # Lists: handle list of dicts or simple list
                if isinstance(data[0], dict):
                    headers = list(data[0].keys())
                    return [headers] + [[row.get(h, '') for h in headers] for row in data]
                else:
                    return [[str(item)] for item in data]
            else:
                # Single value
                return [[str(data)]]
        except Exception as e:
            logger.error(f"Error preparing data for sheets: {e}")
            return [["Error preparing data"]]
    
    def export_data(self, data: Dict[str, Any]) -> bool:
        """Export data to Google Sheets"""
        try:
            if not self._authenticate():
                return False
            
            logger.info(f"Attempting to open spreadsheet with ID: {self.spreadsheet_id}")
            
            # Try to open the spreadsheet
            try:
                spreadsheet = self.client.open_by_key(self.spreadsheet_id)
                logger.info(f"Spreadsheet opened: {spreadsheet.title}")
            except gspread.exceptions.SpreadsheetNotFound:
                logger.error(f"Spreadsheet not found. Please check the ID: {self.spreadsheet_id}")
                logger.info("Creating a new spreadsheet...")
                spreadsheet = self.client.create("Finance Bladi Data")
                # Share with your email if needed
                # spreadsheet.share('your-email@gmail.com', perm_type='user', role='writer')
                self.spreadsheet_id = spreadsheet.id
                logger.info(f"New spreadsheet created with ID: {self.spreadsheet_id}")
            except Exception as e:
                logger.error(f"Error opening spreadsheet: {e}")
                return False
            
            # Export each module's data
            for sheet_name, sheet_data in data.items():
                try:
                    logger.info(f"Exporting {sheet_name}...")
                    
                    # Get or create worksheet
                    try:
                        worksheet = spreadsheet.worksheet(sheet_name)
                        logger.info(f"Using existing worksheet: {sheet_name}")
                    except gspread.exceptions.WorksheetNotFound:
                        logger.info(f"Creating new worksheet: {sheet_name}")
                        worksheet = spreadsheet.add_worksheet(
                            title=sheet_name, 
                            rows=100, 
                            cols=20
                        )
                    
                    # Clear existing data
                    worksheet.clear()
                    
                    # Prepare data
                    values = self._prepare_data_for_sheets(sheet_data)
                    
                    # Update sheet
                    if values:
                        worksheet.update(values, value_input_option='USER_ENTERED')
                        logger.info(f"✓ {sheet_name}: {len(values)} rows exported")
                    
                except Exception as e:
                    logger.error(f"Failed to export {sheet_name}: {e}")
                    logger.debug(traceback.format_exc())
            
            # Create a summary sheet
            try:
                summary_data = [
                    ["Module", "Status", "Timestamp"],
                    ["bkam_forex", "✓ Success", data.get('bkam_forex', {}).get('timestamp', 'N/A')],
                    ["bkam_treasury", "✓ Success", data.get('bkam_treasury', {}).get('timestamp', 'N/A')],
                    ["investing_masi", "✓ Success", data.get('investing_masi', {}).get('timestamp', 'N/A')],
                    ["trading_economics", "✓ Success", data.get('trading_economics', {}).get('timestamp', 'N/A')],
                    ["yahoo_markets", "✓ Success", data.get('yahoo_markets', {}).get('timestamp', 'N/A')],
                ]
                
                try:
                    summary_sheet = spreadsheet.worksheet("Summary")
                except:
                    summary_sheet = spreadsheet.add_worksheet(title="Summary", rows=20, cols=10)
                
                summary_sheet.clear()
                summary_sheet.update(summary_data, value_input_option='USER_ENTERED')
                logger.info("✓ Summary sheet updated")
                
            except Exception as e:
                logger.warning(f"Could not create summary sheet: {e}")
            
            logger.info("✅ All data exported to Google Sheets successfully")
            return True
            
        except Exception as e:
            logger.error(f"Google Sheets export failed: {str(e)}")
            logger.debug(traceback.format_exc())
            return False