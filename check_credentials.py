# check_credentials.py
import json
import os

creds_file = 'credentials.json'
if os.path.exists(creds_file):
    with open(creds_file, 'r') as f:
        creds = json.load(f)
    
    print("✓ Credentials file found")
    print(f"Client Email: {creds.get('client_email')}")
    print(f"Project ID: {creds.get('project_id')}")
    
    # This is the email to share your spreadsheet with
    print(f"\n📧 Share your spreadsheet with this email:")
    print(f"   {creds.get('client_email')}")
else:
    print(f"✗ {creds_file} not found in {os.getcwd()}")