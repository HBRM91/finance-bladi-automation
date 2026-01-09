# Finance Bladi Automation

Automated financial data collection system for Moroccan and global markets.

## 📊 Data Sources
1. **BKAM Forex** - EUR/MAD and USD/MAD rates
2. **BKAM Treasury** - BT2Y, BT5Y, BT10Y bond yields
3. **Investing.com** - MASI stock index
4. **Trading Economics** - Phosphate DAP prices
5. **Yahoo Finance** - 21 global assets (oil, gold, indices, forex)

## 🚀 Setup
```bash
pip install -r requirements.txt
cp credentials.example.json credentials.json  # Add your Google API credentials
python main.py
