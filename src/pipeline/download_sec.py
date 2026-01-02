import os
from sec_edgar_downloader import Downloader

def download_filings(tickers, years, download_dir):
    """
    Downloads 10-K filings for the specified tickers and years.
    
    Args:
        tickers (list): List of stock tickers.
        years (list): List of years to download.
        download_dir (str): Directory where downloaded files will be stored.
    """
    dl = Downloader("FilingLens", "your.email@example.com", download_dir)

    for ticker in tickers:
        print(f"Downloading 10-Ks for {ticker}...")
        for year in years:
             try:
                 dl.get("10-K", ticker, limit=5, download_details=False)
             except Exception as e:
                 print(f"Failed to download for {ticker}: {e}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    RAW_DIR = os.path.join(BASE_DIR, "data/raw_pdfs")
    
    # User requested: AAPL, MSFT, GOOGL, AMZN, NVDA
    TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
    
    print(f"Downloading filings to {RAW_DIR}...")
    download_filings(TICKERS, [], RAW_DIR)
    print("Download complete.")
