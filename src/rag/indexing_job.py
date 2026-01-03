import os
import pandas as pd
from tqdm import tqdm
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
    
    batch_size = 100
    total_chunks = len(df)
    print(f"Indexing {total_chunks} chunks...")

    # Iterate in batches
    for i in tqdm(range(0, total_chunks, batch_size)):
        batch = df.iloc[i : i + batch_size]
        
        vectors = []
        for _, row in batch.iterrows():
            text = row['chunk_text']
            ticker = row['ticker']
            source = row['source_path']
            
            if not text:
                continue
                
            embedding = get_embedding(text)
            
            if embedding:
                # ID can be simplified or hash of content. 
                # Using simple unique ID combination for this demo
                vector_id = f"{ticker}_{i}_{_}" 
                
                vectors.append({
                    "id": vector_id,
                    "values": embedding,
                    "metadata": {
                        "text": text,
                        "ticker": ticker,
                        "source": source
                    }
                })
        
        if vectors:
            index.upsert(vectors=vectors)

    print("Indexing complete!")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    GOLD_PATH = os.path.join(BASE_DIR, "data/gold_chunks")
    
    index_data(GOLD_PATH)
