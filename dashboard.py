# dashboard.py - Simple web view of your data
from flask import Flask, render_template, jsonify
import json
import os
from datetime import datetime

app = Flask(__name__)

# Your project directory
PROJECT_DIR = "/workspaces/finance-bladi-automation"

@app.route('/')
def dashboard():
    """Show latest data in a simple HTML page"""
    # Find latest data file
    data_dir = os.path.join(PROJECT_DIR, 'data')
    json_files = [f for f in os.listdir(data_dir) if f.startswith('raw_')]
    
    if not json_files:
        return "No data found"
    
    latest_file = max(json_files)  # Gets most recent by filename
    file_path = os.path.join(data_dir, latest_file)
    
    # Load data
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Prepare data for display
    display_data = {
        'last_updated': latest_file.replace('raw_', '').replace('.json', ''),
        'forex': {},
        'treasury': {},
        'markets': {}
    }
    
    # Extract BKAM forex
    if 'bkam_forex' in data:
        display_data['forex'] = data['bkam_forex']
    
    # Extract BKAM treasury
    if 'bkam_treasury' in data:
        display_data['treasury'] = data['bkam_treasury']
    
    # Extract key market data
    if 'yahoo_markets' in data:
        yahoo = data['yahoo_markets']
        display_data['markets'] = {
            'BRENT': yahoo.get('BRENT'),
            'WTI': yahoo.get('WTI'),
            'GOLD': yahoo.get('GOLD'),
            'BITCOIN': yahoo.get('BITCOIN'),
            'SP500': yahoo.get('SP500')
        }
    
    return render_template('dashboard.html', data=display_data)

@app.route('/api/latest')
def api_latest():
    """API endpoint for latest data"""
    data_dir = os.path.join(PROJECT_DIR, 'data')
    json_files = [f for f in os.listdir(data_dir) if f.startswith('raw_')]
    
    if not json_files:
        return jsonify({'error': 'No data'})
    
    latest_file = max(json_files)
    file_path = os.path.join(data_dir, latest_file)
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)