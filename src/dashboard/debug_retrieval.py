
import os
import sys
from dotenv import load_dotenv

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.rag.vector_db import get_index

def debug_pinecone():
    print("Connecting to Pinecone...")
    index = get_index()
    
    print("Fetching stats...")
    stats = index.describe_index_stats()
    print(f"Index stats: {stats}")
    
    # List IDs to get a valid ID
    print("\nListing headers (IDs) to find a valid vector...")
    # List is not available in standard client widely without iteration, 
    # but we can try a query with a random vector or just all-zeros might have failed due to normalization.
    # Let's try querying with a random vector.
    import random
    dummy_vec = [random.random() for _ in range(768)]
    
    results = index.query(
        vector=dummy_vec,
        top_k=5,
        include_metadata=True
    )
    
    if results['matches']:
        print(f"Found {len(results['matches'])} matches with random vector.")
        match = results['matches'][0]
        meta = match['metadata']
        print(f"Sample ID: {match['id']}")
        print(f"Sample Metadata: {meta}")
        print(f"Year type: {type(meta.get('year'))}")
        print(f"Ticker type: {type(meta.get('ticker'))}")
        
        # Check what values are actually in there
        years_found = set(m['metadata'].get('year') for m in results['matches'])
        print(f"Years found in top 5: {years_found}")
        
    else:
        print("No vectors found even with random query!")
        return

    # specific test for AAPL 2022 using the SERVICE LAYER (to test the fix)
    print("\nTest Filtering for AAPL 2022 (Risk Factors) via search_filings...")
    
    from src.rag.retrieval_service import search_filings
    
    # Try String Year (Should now work due to fix)
    print("Calling search_filings with year='2022' (str)...")
    results = search_filings("risk", ticker="AAPL", year="2022", section="Risk Factors", top_k=1)
    
    print(f"search_filings found {len(results)} matches.")
    if results:
         print(f"First match year: {results[0]['year']} (type: {type(results[0]['year'])})")
    else:
         print("Still no matches!")

if __name__ == "__main__":
    debug_pinecone()
