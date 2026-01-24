import os
import sys
from sec_edgar_downloader import Downloader

def download_filings():
    # Use absolute path
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    download_dir = os.path.join(base_dir, "data", "raw_pdfs")
    
    # 1. Initialize Downloader with valid User Agent structure
    # SEC requires "Name email@domain.com"
    dl = Downloader("FilingLens", "filinglens_user@example.com", download_dir)
    
    tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
    
    # Limit number of filings per ticker to save time/space
    limit = 5 
    
    print(f"Downloading filings to {download_dir}...")
    
    for ticker in tickers:
        print(f"Downloading 10-Ks for {ticker}...")
        try:
            # get() returns number of files downloaded
            count = dl.get("10-K", ticker, limit=limit, download_details=True)
            print(f"  -> Downloaded {count} filings for {ticker}")
        except Exception as e:
            print(f"  -> Failed to download {ticker}: {e}")

    print("Download complete.")

if __name__ == "__main__":
    download_filings()
