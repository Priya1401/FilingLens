import streamlit as st
import os
import sys
import sys
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Load Env
def get_genai_model(model_name='gemini-2.0-flash'):
    import google.generativeai as genai
    load_dotenv()
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    if GOOGLE_API_KEY:
        genai.configure(api_key=GOOGLE_API_KEY)
    return genai.GenerativeModel(model_name)
    
# --- GEMINI GENERATION ---
def generate_answer(query, context_chunks):
    """
    Synthesizes an answer using Gemini given the query and retrieved context.
    """
    import google.generativeai as genai
    model = get_genai_model('gemini-2.0-flash')
    
    # Construct Prompt
    context_text = "\n\n".join([f"[Source: {c['ticker']} {c['year']} - {c['section']}]\n{c['text']}" for c in context_chunks])
    
    prompt = f"""
    You are an expert financial analyst assistant. Use the provided context from SEC filings to answer the user's question.
    
    Context:
    {context_text}
    
    Question: {query}
    
    Instructions:
    - Answer primarily based on the context.
    - If the context doesn't contain the answer, say "I couldn't find specific information in the filings."
    - Be professional, concise, and accurate.
    - Cite the ticker and year if comparing companies.
    """
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Error generating answer: {e}"

def generate_comparison(text1, text2, ticker, year1, year2, section):
    """
    Generates a summary of material changes between two texts.
    """
    import google.generativeai as genai
    model = get_genai_model('gemini-2.0-flash')
    
    prompt = f"""
    You are a refined financial analyst. Compare the following two text blocks from {ticker}'s {section} in {year1} vs {year2}.
    
    Text 1 ({year1} - Old):
    {text1[:15000]} # Truncate to avoid context window issues if very large
    
    Text 2 ({year2} - New):
    {text2[:15000]}
    
    Task:
    - Identify **MATERIAL** changes in strategy, risk, or financial outlook.
    - Ignore minor wording changes or date updates.
    - Highlight specific paragraphs that were added or removed if they are significant.
    - Provide a bulleted summary of the "Delta".
    """
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Error generating comparison: {e}"

# --- STREAMLIT UI ---
st.set_page_config(page_title="FilingLens", page_icon="📊", layout="wide")

st.title("📊 FilingLens: AI Financial Analyst")
st.markdown("Chat with SEC 10-K Filings & Compare YoY Changes (Powered by RAG + Gemini)")

# Sidebar
with st.sidebar:
    st.header("Settings")
    # Ticker Filter
    tickers = ["All", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
    selected_ticker = st.selectbox("Filter by Ticker", tickers)
    
    # Metadata Filters for Chat
    st.subheader("Chat Filters")
    years = ["All", "2024", "2023", "2022", "2021", "2020"]
    selected_year = st.selectbox("Filter by Year", years)
    
    sections = ["All", "Risk Factors", "MD&A", "Full Report"]
    selected_section = st.selectbox("Filter by Section", sections)
    
    st.markdown("---")
    st.markdown("### About")
    st.info("This tool uses a RAG pipeline to retrieve relevant sections from recent 10-K filings and synthesizes answers using Gemini.")

# Tabs
tab1, tab2 = st.tabs(["💬 Chat", "🔄 YoY Comparison"])

with tab1:
    # Chat State
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Display History
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if "citations" in message and message["citations"]:
                with st.expander("View Cited Sources"):
                    for c in message["citations"]:
                        st.markdown(f"**[{c['ticker']} {c['year']}]** ({c['section']}) ...{c['text'][:200]}...")
    
    # Chat Input
    if prompt := st.chat_input("Ask a question about financial performance, risks, or strategy..."):
        # User Message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Assistant Response
        with st.chat_message("assistant"):
            with st.spinner("Analyzing filings..."):
                # 1. Retrieve
                ticker_filter = None if selected_ticker == "All" else selected_ticker
                year_filter = None if selected_year == "All" else selected_year
                section_filter = None if selected_section == "All" else selected_section
                
                # Map "Full Report" to None or implementation specific
                if section_filter == "Full Report": section_filter = None
                
                # Lazy import
                from src.rag.retrieval_service import search_filings
                
                retrieved_chunks = search_filings(
                    prompt, 
                    ticker=ticker_filter, 
                    year=year_filter,
                    section=section_filter,
                    top_k=5
                )
                
                # 2. Generate
                if retrieved_chunks:
                    answer = generate_answer(prompt, retrieved_chunks)
                else:
                    answer = "I couldn't find any relevant information in the indexed filings."
                
                st.markdown(answer)
                
                # 3. Citations
                if retrieved_chunks:
                    with st.expander("View Cited Sources"):
                        for c in retrieved_chunks:
                            st.markdown(f"**[{c['ticker']} {c['year']}]** ({c['section']}) ...{c['text'][:200]}...")
        
        # Save Assistant Message
        st.session_state.messages.append({
            "role": "assistant", 
            "content": answer,
            "citations": retrieved_chunks
        })

with tab2:
    st.header("Year-Over-Year Comparison")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        comp_ticker = st.selectbox("Ticker", ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"], key="comp_ticker")
    with col2:
        comp_section = st.selectbox("Section", ["Risk Factors", "MD&A"], key="comp_section")
    with col3:
        year1 = st.selectbox("Year 1 (Baseline)", ["2023", "2022", "2021", "2020"], index=1, key="y1")
    with col4:
        year2 = st.selectbox("Year 2 (Current)", ["2024", "2023", "2022"], index=0, key="y2")
        
    if st.button("Compare"):
        with st.spinner("Retrieving and Comparing..."):
            # Lazy import
            from src.rag.retrieval_service import get_section_text

            text1 = get_section_text(comp_ticker, year1, comp_section)
            text2 = get_section_text(comp_ticker, year2, comp_section)
            
            if not text1 or not text2:
                st.error(f"Could not find data for {comp_ticker} in {year1} or {year2} for section {comp_section}.")
            else:
                st.subheader(f"Analysis: {comp_ticker} {comp_section} ({year1} vs {year2})")
                
                # AI Analysis
                analysis = generate_comparison(text1, text2, comp_ticker, year1, year2, comp_section)
                st.markdown(analysis)
                
                with st.expander("View Raw Text"):
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown(f"**{year1}**")
                        st.text(text1[:2000] + "...")
                    with c2:
                        st.markdown(f"**{year2}**")
                        st.text(text2[:2000] + "...")
