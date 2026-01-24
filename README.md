# FilingLens: Enterprise-Grade SEC Filing Analysis Agent 

**FilingLens** is a scalable, domain-specific RAG (Retrieval-Augmented Generation) platform designed to ingest, analyze, and compare financial documents at scale.

Building to solve the challenge of **unstructured financial data**, it will leverage a distributed **PySpark** architecture to process 10+ years of SEC filings (10-Ks, 10-Qs) and use **Gemini 2.0 Flash** to detect material changes in risk factors and financial disclosures year-over-year.

---

## Key Features

* **Scalable Knowledge Pipeline (PySpark):** Utilize a distributed ETL job to clean, chunk, and embed thousands of PDF pages in parallel.
* **Comparative Analysis Engine:** Specialized reasoning logic to retrieve and compare distinct sections (e.g., "Risk Factors") across different years to highlight regulatory shifts.
* **Zero-Hallucination Citations:** The reasoning engine will be strictly prompted to cite specific page numbers and paragraphs for every claim, ensuring auditability for financial compliance.
* **Multi-Modal Reasoning:** Leverage **Google Gemini 2.0 Flash** to process long-context windows, enabling deep semantic understanding of complex financial legal jargon.

---

## Getting Started

Follow these instructions to set up and run the project locally.

### Prerequisites

*   **Python 3.10+**
*   **Git**

### Installation

1.  Clone the repository:
    ```bash
    git clone <repository_url>
    cd FilingLens
    ```

2.  Create and activate a virtual environment:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

### Configuration

1.  Create a `.env` file in the root directory.
2.  Add the following API keys:

    ```env
    GOOGLE_API_KEY=your_google_gemini_api_key
    PINECONE_API_KEY=your_pinecone_api_key
    ```

### Usage

#### 1. Indexing Data
To process the SEC filings and populate the vector database (Pinecone), run the indexing job:

```bash
python src/rag/indexing_job.py
```
*Note: This script currently looks for Parquet files in `data/parquet`.*

#### 2. Running the Dashboard
To launch the Streamlit interface for chatting with filings and comparing data:

```bash
streamlit run src/dashboard/app.py
```
The app will open in your browser at `http://localhost:8501`.

---
