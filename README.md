# FilingLens: Enterprise-Grade SEC Filing Analysis Agent 

**FilingLens** is a scalable, domain-specific RAG (Retrieval-Augmented Generation) platform designed to ingest, analyze, and compare financial documents at scale.

Built to solve the challenge of **unstructured financial data**, it leverages a distributed **PySpark** architecture to process 10+ years of SEC filings (10-Ks, 10-Qs) and uses **Gemini 1.5** to detect material changes in risk factors and financial disclosures year-over-year.

---

## Key Features

* **Scalable Knowledge Pipeline (PySpark):** Utilizes a distributed ETL job to clean, chunk, and embed thousands of PDF pages in parallel.
* **Comparative Analysis Engine:** Specialized reasoning logic to retrieve and compare distinct sections (e.g., "Risk Factors") across different years to highlight regulatory shifts.
* **Zero-Hallucination Citations:** The reasoning engine is strictly prompted to cite specific page numbers and paragraphs for every claim, ensuring auditability for financial compliance.
* **Multi-Modal Reasoning:** Leverages **Google Gemini 1.5 Pro** to process long-context windows, enabling deep semantic understanding of complex financial legal jargon.

---
