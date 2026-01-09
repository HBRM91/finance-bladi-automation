#!/usr/bin/env python3
"""
Simple Python Scheduler for Finance Bladi Automation
Runs daily at 7:00 AM
"""

import schedule
import time
import subprocess
import logging
from datetime import datetime
import os

# Setup logging
log_dir = "/workspaces/finance-bladi-automation/logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{log_dir}/scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_finance_bladi():
    """Run the main automation script"""
    try:
        logger.info("🚀 Starting Finance Bladi Automation...")
        
        # Change to project directory
        os.chdir("/workspaces/finance-bladi-automation")
        
        # Run the main script
        result = subprocess.run(
            ["python3", "main.py"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            logger.info("✅ Automation completed successfully")
            logger.info(f"Output: {result.stdout[-500:]}")  # Last 500 chars
        else:
            logger.error(f"❌ Automation failed with code {result.returncode}")
            logger.error(f"Error: {result.stderr}")
        
    except Exception as e:
        logger.error(f"❌ Scheduler error: {e}")

def main():
    """Main scheduler function"""
    print("="*60)
    print("📅 FINANCE BLADI SCHEDULER")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Will run daily at 7:00 AM")
    print("Press Ctrl+C to stop")
    print("="*60)
    
    # Install schedule if not installed
    try:
        import schedule
    except ImportError:
        print("Installing schedule module...")
        subprocess.run(["pip", "install", "schedule"])
        import schedule
    
    # Schedule the job
    schedule.every().day.at("07:00").do(run_finance_bladi)
    
    # Also run immediately on startup (optional)
    print("Running initial execution...")
    run_finance_bladi()
    
    # Keep running
    while True:
        schedule.run_pending()
        
        # Check if it's close to 9 AM
        now = datetime.now()
        if now.hour == 8 and now.minute == 59:
            logger.info("⏰ One minute until scheduled run...")
        
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Scheduler stopped by user")
    except Exception as e:
        print(f"❌ Scheduler crashed: {e}")