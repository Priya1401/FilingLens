import os
import sys
import pandas as pd
from tqdm import tqdm

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.rag.embeddings import get_embedding
from src.rag.vector_db import get_index

def index_data(parquet_path):
    print(f"Loading data from {parquet_path}...")
    try:
        df = pd.read_parquet(parquet_path, engine='pyarrow')
    except Exception as e:
        print(f"Error reading Parquet: {e}")
        return

    index = get_index()
    
    # Clear existing index to avoid schema conflicts
    print("Clearing existing vectors...")
    try:
        index.delete(delete_all=True)
    except Exception as e:
        print(f"Index clear failed (maybe empty): {e}")
    
    batch_size = 10
    total_rows = len(df)
    print(f"Indexing {total_rows} rows (will be chunked)...")

    def recursive_chunker(text, chunk_size=4000, overlap=200):
        if len(text) <= chunk_size:
            return [text]
        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunks.append(text[start:end])
            start += (chunk_size - overlap)
        return chunks

    # Iterate in batches
    for i in tqdm(range(0, total_rows, batch_size)):
        batch = df.iloc[i : i + batch_size]
        
        vectors = []
        for _, row in batch.iterrows():
            full_text = row['text']
            ticker = row['ticker']
            year = row['year']
            section = row['section']
            
            if not full_text:
                continue
            
            # CHUNK THE TEXT
            # Pinecone limit is ~40KB metadata. 
            # We use 4000 chars (~4KB) to be safe and allows multiple chunks per section
            chunks = recursive_chunker(full_text)
            
            for chunk_idx, chunk_text in enumerate(chunks):
                if not chunk_text.strip(): 
                    continue
                    
                embedding = get_embedding(chunk_text)
                
                if embedding:
                    # ID: Ticker_Year_Section_RowIndex_ChunkIndex
                    vector_id = f"{ticker}_{year}_{section}_{_}_{chunk_idx}" 
                    
                    vectors.append({
                        "id": vector_id,
                        "values": embedding,
                        "metadata": {
                            "text": chunk_text,
                            "ticker": ticker,
                            "year": year,
                            "section": section,
                            "chunk_index": chunk_idx
                        }
                    })
        
        if vectors:
            # Upsert in batches of 100 vectors max to be safe
            # Slice vectors list
            v_batch_size = 50
            for k in range(0, len(vectors), v_batch_size):
                 index.upsert(vectors=vectors[k : k + v_batch_size])

    print("Indexing complete!")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    GOLD_PATH = os.path.join(BASE_DIR, "data/parquet")
    
    index_data(GOLD_PATH)
