import os
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY not found in environment variables.")

genai.configure(api_key=GOOGLE_API_KEY)

def get_embedding(text, model="models/text-embedding-004"):
    """
    Generates embedding for a given text using Gemini.
    """
    try:
        # text-embedding-004 supports retrieval_document and retrieval_query task types
        # For simplicity in this wrapper we default to the model's auto behavior or specify query
        result = genai.embed_content(
            model=model,
            content=text,
            task_type="retrieval_document"
        )
        return result['embedding']
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None

def get_query_embedding(text, model="models/text-embedding-004"):
    """
    Generates embedding for a query (optimized for retrieval matching).
    """
    try:
        result = genai.embed_content(
            model=model,
            content=text,
            task_type="retrieval_query"
        )
        return result['embedding']
    except Exception as e:
        print(f"Error generating query embedding: {e}")
        return None
