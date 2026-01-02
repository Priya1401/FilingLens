# FilingLens: Enterprise-Grade SEC Filing Analysis Agent 

**FilingLens** is a scalable, domain-specific RAG (Retrieval-Augmented Generation) platform designed to ingest, analyze, and compare financial documents at scale.

Building to solve the challenge of **unstructured financial data**, it will leverage a distributed **PySpark** architecture to process 10+ years of SEC filings (10-Ks, 10-Qs) and use **Gemini 1.5** to detect material changes in risk factors and financial disclosures year-over-year.

---

## Key Features

* **Scalable Knowledge Pipeline (PySpark):** Utilize a distributed ETL job to clean, chunk, and embed thousands of PDF pages in parallel.
* **Comparative Analysis Engine:** Specialized reasoning logic to retrieve and compare distinct sections (e.g., "Risk Factors") across different years to highlight regulatory shifts.
* **Zero-Hallucination Citations:** The reasoning engine will be strictly prompted to cite specific page numbers and paragraphs for every claim, ensuring auditability for financial compliance.
* **Multi-Modal Reasoning:** Leverage **Google Gemini 1.5** to process long-context windows, enabling deep semantic understanding of complex financial legal jargon.

---
