import os
import time
import requests
import schedule
import logging
import apprise
from apprise import Apprise
from loguru import logger
from datetime import datetime

import json

# Configuration
def get_env(key):
    val = os.getenv(key)
    if not val:
        raise ValueError(f"Missing required environment variable: {key}")
    return val

USER_WALLET = get_env("USER_WALLET")
CHECK_INTERVAL = int(get_env("CHECK_INTERVAL"))
# LTV_THRESHOLD_PCT = float(get_env("LTV_THRESHOLD_PCT")) # Deprecated for alerting, but kept if needed in future
HF_THRESHOLD = float(get_env("HF_THRESHOLD"))

DATA_DIR = "/app/data"
CACHE_FILE = os.path.join(DATA_DIR, "active_markets.json")

# Kamino Markets (Name -> Pubkey)
def fetch_all_markets_config():
    """
    Fetch all Kamino lending markets config from the V2 API.
    Returns a dict of {market_name: market_pubkey} for ALL markets.
    """
    logger.info("Fetching all markets list from Kamino API...")
    url = "https://api.kamino.finance/v2/kamino-market"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            markets = {}
            for m in data:
                name = m.get("name", "Unknown Market")
                pubkey = m.get("lendingMarket")
                if pubkey:
                    markets[name] = pubkey
            logger.info(f"API returned {len(markets)} total potential markets.")
            return markets
    except Exception as e:
        logger.error(f"Failed to fetch markets: {e}")
    return {}

def scan_and_cache_markets(resolved_wallet):
    """
    Scans ALL available markets to find where the user has positions.
    Saves the list of active markets to CACHE_FILE.
    Returns the dict of active markets.
    """
    all_markets = fetch_all_markets_config()
    if not all_markets:
        logger.warning("No markets found via API. Returning empty list.")
        return {}

    logger.info(f"Scanning {len(all_markets)} markets for active positions (this may take a while)...")
    active_markets = {}

    for name, pubkey in all_markets.items():
        # Check if user has obligation
        try:
            # We use a short timeout here to speed up the scan
            obligations = get_obligations(pubkey, resolved_wallet)
            has_position = False
            for obl in obligations:
                # Parse Metrics from refreshedStats
                stats = obl.get('refreshedStats', {})
                deposited = float(stats.get('userTotalDeposit', 0))
                borrowed = float(stats.get('userTotalBorrow', 0))
                
                if deposited > 0.1 or borrowed > 0.1:
                    has_position = True
                    break
            
            if has_position:
                logger.info(f"Found active position in: {name}")
                active_markets[name] = pubkey
            else:
                # logger.debug(f"No position in {name}")
                pass
                
            # Rate limit politeness
            # Kamino API might have limits (approx 90/min). 
            # 28 markets * 1s = 28s scan time. Safe.
            time.sleep(1.0) 
            
        except Exception as e:
            logger.warning(f"Error checking {name}: {e}")

    # Save to cache
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(CACHE_FILE, 'w') as f:
            json.dump(active_markets, f, indent=2)
        logger.info(f"Saved {len(active_markets)} active markets to {CACHE_FILE}")
    except Exception as e:
        logger.error(f"Failed to save cache file: {e}")

    return active_markets

def load_cached_markets():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load cache: {e}")
    return None

# Apprise Setup
apobj = Apprise()
# Load from the mounted configuration file
config_path = "/etc/apprise/apprise.yml"
if os.path.exists(config_path):
    logger.info(f"Loading Apprise config from {config_path}")
    config = apprise.AppriseConfig()
    config.add(config_path)
    apobj.add(config)
else:
    logger.warning(f"Apprise config not found at {config_path}. Notifications will not be sent.")

def resolve_sns(domain):
    """
    Attempt to resolve .sol domain to Pubkey.
    Uses a public API (e.g., Bonfida SNS proxy) if available, 
    otherwise returns the domain and logs a warning.
    """
    if not domain.endswith(".sol"):
        return domain
    
    logger.info(f"Attempting to resolve SNS: {domain}...")
    try:
        # Using Bonfida's public API for resolution
        response = requests.get(f"https://sns-sdk-proxy.bonfida.workers.dev/resolve/{domain}")
        if response.status_code == 200:
            data = response.json()
            if data.get("result"):
                pubkey = data["result"]
                logger.info(f"Resolved {domain} to {pubkey}")
                return pubkey
    except Exception as e:
        logger.error(f"Failed to resolve SNS: {e}")
    
    logger.warning(f"Could not resolve {domain}. Please provide the PubKey directly in USER_WALLET.")
    return domain

def get_obligations(market_pubkey, user_pubkey):
    url = f"https://api.kamino.finance/kamino-market/{market_pubkey}/users/{user_pubkey}/obligations"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.error(f"Error fetching obligations for market {market_pubkey}: {e}")
    return []

# State for Daily Report
LAST_SUMMARY_DATE = None

def monitor_job(is_startup=False):
    global LAST_SUMMARY_DATE
    logger.info("Starting monitor cycle...")
    
    # 1. Resolve Wallet
    try:
        resolved_wallet = resolve_sns(USER_WALLET)
    except:
        resolved_wallet = USER_WALLET

    # Startup & Daily Summary Containers
    startup_lines = []
    
    # 2. Check Daily Summary Condition (After 18:00)
    now = datetime.now()
    send_daily_summary = False
    daily_summary_lines = []
    
    if now.hour >= 18 and (LAST_SUMMARY_DATE != now.date()):
        send_daily_summary = True
        logger.info("🕒 It is after 18:00. Preparing Daily Summary...")

    for market_name, market_pubkey in MARKETS.items():
        logger.info(f"Checking {market_name} ({market_pubkey})...")
        obligations = get_obligations(market_pubkey, resolved_wallet)
        
        if not obligations:
            logger.debug(f"No obligations found in {market_name}")
            continue
            
        for obl in obligations:
            # Structure of obligation response varies, need to handle carefully
            # Usually contains 'loan_to_value_ratio', 'health_factor', etc.
            # Assuming the API returns a list of obligation objects
            
            # Example parsing (fallback to keys if exact struc unknown)
            # Kamino API v2 usually returns detailed metrics
            
            # Simulated checks based on API standard fields
            current_ltv = float(obl.get('loan_to_value_pct', 0)) # Often in % or decimal, assume % if > 1
            if current_ltv < 1 and current_ltv > 0: current_ltv *= 100 # Normalize to %
            
            # Calculate or Get Health Factor
            # Sometimes HF is not direct, but we can try 'current_health_factor'
            hf = float(obl.get('current_health_factor', 999))
            
            # If API doesn't return calc fields, we might need raw data math
            # But let's assume the API provides these metrics as per standard
            
            # Check if this particular obligation has active borrowing or collateral
            # (Filtering out empty or dust positions)
            stats = obl.get('refreshedStats', {})
            deposited = float(stats.get('userTotalDeposit', 0))
            borrowed = float(stats.get('userTotalBorrow', 0))
            
            # If no significant deposit or borrow, skip
            if deposited < 0.1 and borrowed < 0.1:
               continue

            # Calculate Metrics
            # LTV is usually 0.xx in API, convert to %
            current_ltv = float(stats.get('loanToValue', 0)) * 100
            liquidation_ltv = float(stats.get('liquidationLtv', 0)) * 100
            
            # Health Factor Calculation
            # HF = Liquidation Limit / Total Borrowed
            liq_limit = float(stats.get('borrowLiquidationLimit', 0))
            if borrowed > 0:
                hf = liq_limit / borrowed
            else:
                hf = 999.0
            
            # Log current LTV/HF to CLI as requested
            status_msg = (
                f"[{market_name}] Status:\n"
                f"  - Current LTV: {current_ltv:.2f}%\n"
                f"  - Liquidation LTV: {liquidation_ltv:.2f}%\n"
                f"  - Health Factor: {hf:.2f}"
            )
            logger.info(status_msg.replace("\n", " | ")) # Single line for CLI log
            
            if send_daily_summary:
                daily_summary_lines.append(status_msg)
            
            if is_startup:
                startup_lines.append(status_msg)

            alerts = []
            # Only alert on Health Factor as requested
            if hf <= HF_THRESHOLD:
                alerts.append(f"⚠️ Health Factor Low: {hf:.2f} (Limit: {HF_THRESHOLD})")
                
            if alerts:
                # Add context to alert
                msg = (
                    f"🚨 Kamino Alert: {market_name}\n"
                    f"{alerts[0]}\n\n"
                    f"Risk Details:\n"
                    f"📉 Current LTV: {current_ltv:.2f}%\n"
                    f"💀 Liquidation LTV: {liquidation_ltv:.2f}%"
                )
                logger.warning(msg)
                apobj.notify(
                    body=msg,
                    title=f"Kamino Risk Alert: {market_name}",
                    tag="ST"
                )

    # 3. Send Daily Summary if collected
    if send_daily_summary:
        if daily_summary_lines:
            summary_body = "📅 Daily Kamino Summary (18:00+):\n\n" + "\n".join(daily_summary_lines)
            logger.info("Sending Daily Summary Notification...")
            apobj.notify(
                body=summary_body,
                title="Kamino Daily Report",
                tag="ST"
            )
        else:
            logger.info("Daily Summary: No active positions found to report.")
            
        # Mark as done for today
        LAST_SUMMARY_DATE = now.date()

    # 4. Send Startup Report
    if is_startup:
        logger.info("Sending Startup Report...")
        if startup_lines:
            body = f"🚀 Kamino Monitor Started\nWallet: {USER_WALLET}\nInterval: {CHECK_INTERVAL}s\nHF Threshold: < {HF_THRESHOLD}\n\n📌 Current Holdings:\n\n" + "\n\n".join(startup_lines)
        else:
            body = f"🚀 Kamino Monitor Started\nWallet: {USER_WALLET}\nInterval: {CHECK_INTERVAL}s\n\nNo active positions found."
        
        apobj.notify(
            body=body,
            title="Kamino Startup Report",
            tag="ST"
        )

def main():
    global MARKETS
    logger.info(f"Kamino Monitor Initializing... Wallet: {USER_WALLET}")
    
    # Resolve wallet once
    try:
        resolved_wallet = resolve_sns(USER_WALLET)
    except:
        resolved_wallet = USER_WALLET
    
    # Notification - Startup
    # (Moved to monitor_job(is_startup=True) to include position details)
    # apobj.notify(
    #     body=f"Kamino Monitor Started.\nWallet: {USER_WALLET}\nInterval: {CHECK_INTERVAL}s",
    #     title="Kamino Monitor Up",
    #     tag="ST"
    # )

    # Market Discovery Logic
    cached = load_cached_markets()
    if cached is not None:
        logger.info(f"Loaded {len(cached)} markets from cache ({CACHE_FILE}).")
        MARKETS = cached
    else:
        logger.info("No cache found. Performing full scan of all markets...")
        MARKETS = scan_and_cache_markets(resolved_wallet)
    
    if not MARKETS:
        logger.warning("No active markets found to monitor! Waiting for manual intervention or restart.")
    else:
        logger.info(f"Monitoring {len(MARKETS)} active markets: {list(MARKETS.keys())}")
        monitor_job(is_startup=True) # Run active check immediately with startup report

    schedule.every(CHECK_INTERVAL).seconds.do(monitor_job)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
