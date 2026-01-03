from src.rag.embeddings import get_query_embedding
from src.rag.vector_db import get_index

def search_filings(query, ticker=None, top_k=5):
    """
    Semantic search over filings.
    Args:
        query (str): The search query.
        ticker (str, optional): Filter by ticker symbol.
        top_k (int): Number of results.
    """
    index = get_index()
    
    # Generate query embedding
    query_vec = get_query_embedding(query)
    if not query_vec:
        return []
    
    # Construct filter if ticker provided
    filter_dict = {}
    if ticker:
        filter_dict["ticker"] = ticker
        
    # Query Pinecone
    results = index.query(
        vector=query_vec,
        top_k=top_k,
        include_metadata=True,
        filter=filter_dict if filter_dict else None
    )
    
    # Format results
    matches = []
    for match in results['matches']:
        matches.append({
            "score": match['score'],
            "text": match['metadata']['text'],
            "ticker": match['metadata']['ticker'],
            "source": match['metadata']['source']
        })
        
    return matches

if __name__ == "__main__":
    # Test
    q = "What are the primary risk factors regarding AI?"
    print(f"Query: {q}")
    results = search_filings(q, ticker="AAPL")
    for r in results:
        print(f"[{r['score']:.2f}] {r['text'][:100]}...")
