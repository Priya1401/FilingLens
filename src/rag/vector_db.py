import os
import time
from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv

load_dotenv()

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME = "filinglens-index"

if not PINECONE_API_KEY:
    raise ValueError("PINECONE_API_KEY not found in environment variables.")

pc = Pinecone(api_key=PINECONE_API_KEY)

def get_index():
    """
    Returns the Pinecone index, creating it if it doesn't exist.
    """
    if INDEX_NAME not in pc.list_indexes().names():
        print(f"Index '{INDEX_NAME}' not found. Creating...")
        # Dimension 768 is standard for text-embedding-004
        pc.create_index(
            name=INDEX_NAME,
            dimension=768, 
            metric="cosine",
            spec=ServerlessSpec(
                cloud="aws",
                region="us-east-1"
            )
        )
        # Wait for index to be ready
        while not pc.describe_index(INDEX_NAME).status['ready']:
            time.sleep(1)
            
    return pc.Index(INDEX_NAME)
