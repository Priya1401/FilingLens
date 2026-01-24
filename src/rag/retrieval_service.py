from src.rag.embeddings import get_query_embedding
from src.rag.vector_db import get_index

def search_filings(query, ticker=None, year=None, section=None, top_k=5):
    """
    Semantic search over filings.
    Args:
        query (str): The search query.
        ticker (str, optional): Filter by ticker symbol.
        year (str, optional): Filter by year.
        section (str, optional): Filter by section (Risk Factors, MD&A).
        top_k (int): Number of results.
    """
    index = get_index()
    
    # Generate query embedding
    query_vec = get_query_embedding(query)
    if not query_vec:
        return []
    
    # Construct filter
    filter_dict = {}
    if ticker:
        filter_dict["ticker"] = ticker
    if year:
        try:
            filter_dict["year"] = int(year)
        except ValueError:
            # Fallback if year is not an integer string (e.g. "2022-Q1"?)
            filter_dict["year"] = year
    if section:
        filter_dict["section"] = section
        
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
            "year": match['metadata'].get('year', 'N/A'),
            "section": match['metadata'].get('section', 'N/A')
        })
        
    return matches

def get_section_text(ticker, year, section):
    """
    Retrieves the full text for a specific section.
    Since we don't have a SQL DB, we query Pinecone with a generic vector 
    and permissive top_k to get the chunks.
    """
    # Generic query to get relevant chunks
    # In a real system, we'd iterate or use fetch if we knew IDs
    results = search_filings(
        query=f"Full text of {section}", 
        ticker=ticker, 
        year=year, 
        section=section, 
        top_k=50 # Attempt to get all chunks for the section
    )
    
    # Sort chunks? 
    # Current ingestion doesn't strictly preserve order in vectors, 
    # but we can join them. For a diff summary, strict order is nice but 
    # bag-of-chunks is often "okay" for LLM summarization.
    
    full_text = "\n\n".join([r['text'] for r in results])
    return full_text

