import os
import io
import re
import sys
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, element_at, split, explode, struct, array
from pyspark.sql.types import StringType, ArrayType, StructType, StructField

# Ensure driver and worker use the same python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def create_spark_session():
    """Initializes a Spark session suitable for local development."""
    spark = SparkSession.builder \
        .appName("FilingLens_Ingestion_Silver") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    return spark

# --- UDFS ---

def parse_html(file_content):
    """
    Extracts text from HTML bytes using BeautifulSoup.
    """
    try:
        # Convert bytes to string (assume utf-8)
        html_str = file_content.decode('utf-8', errors='ignore')
        soup = BeautifulSoup(html_str, 'html.parser')
        
        # Get text
        text = soup.get_text(separator=' ')
        return text
    except Exception as e:
        return f"Error parsing HTML: {str(e)}"

# Register UDF
parse_html_udf = udf(parse_html, StringType())

def clean_text(text):
    """
    Cleans the extracted text:
    - Removes Table of Contents line references.
    - Normalizes whitespace.
    """
    if not text:
        return ""
    
    # 1. Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # 2. Remove "Table of Contents" references (heuristic)
    text = re.sub(r'Table of Contents', '', text, flags=re.IGNORECASE)
    
    return text

clean_text_udf = udf(clean_text, StringType())

def extract_year(text):
    """
    Extracts the fiscal year end from the text.
    Heuristic: Looks for "fiscal year ended December 31, 20XX"
    """
    if not text:
        return "Unknown"
    
    # Regex to find year near "fiscal year ended"
    # Matches: "fiscal year ended December 31, 2023" or "fiscal year ended January 28, 2023"
    match = re.search(r'fiscal\s+year\s+ended\s+[A-Za-z]+\s+\d{1,2},\s+(\d{4})', text, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return "Unknown"

extract_year_udf = udf(extract_year, StringType())

# Define Return Type for Split Sections
# Array of Structs: [{section_name: "Risk Factors", content: "..."}]
section_schema = ArrayType(StructType([
    StructField("section_name", StringType(), False),
    StructField("content", StringType(), False)
]))

def split_sections(text):
    """
    Splits text into sections:
    - Item 1A. Risk Factors
    - Item 7. Management's Discussion and Analysis
    - Other
    """
    if not text:
        return []
    
    sections = []
    
    # 1. Find indices (Very naive heuristic)
    # Using specific headers usually found in cleaned text
    
    # Note: In a real robust system, we would use regex with more flexibility
    # For now, we search for the standard header strings
    
    # Risk Factors
    risk_pattern = r'Item\s+1A\.?\s+Risk\s+Factors'
    mda_pattern = r'Item\s+7\.?\s+Management'
    
    risk_matches = list(re.finditer(risk_pattern, text, re.IGNORECASE))
    mda_matches = list(re.finditer(mda_pattern, text, re.IGNORECASE))
    
    # We want the LAST occurrence usually because the first might be TOC
    risk_start = -1
    if risk_matches:
        risk_start = risk_matches[-1].end()
        
    mda_start = -1
    if mda_matches:
        mda_start = mda_matches[-1].end()
    
    # Heuristic: Risk usually comes before MD&A. 
    # Grab Risk Factors
    if risk_start != -1:
        # Determine end of Risk Factors
        end_risk = mda_start if mda_start != -1 and mda_start > risk_start else risk_start + 50000 # Fallback length
        risk_content = text[risk_start:end_risk]
        if len(risk_content) > 100:
             sections.append({"section_name": "Risk Factors", "content": risk_content})
             
    # Grab MD&A
    if mda_start != -1:
        # End of MD&A is usually Item 7A or Item 8
        next_item_pattern = r'Item\s+(7A|8)\.?'
        next_matches = list(re.finditer(next_item_pattern, text, re.IGNORECASE))
        mda_end = -1
        
        # Find first match AFTER mda_start
        for m in next_matches:
            if m.start() > mda_start:
                mda_end = m.start()
                break
        
        if mda_end == -1:
             mda_end = mda_start + 50000 # Fallback
             
        mda_content = text[mda_start:mda_end]
        if len(mda_content) > 100:
             sections.append({"section_name": "MD&A", "content": mda_content})
    
    # If no sections found, treat whole as "Full Report"
    if not sections:
        sections.append({"section_name": "Full Report", "content": text})
        
    return sections

split_sections_udf = udf(split_sections, section_schema)


def run_ingestion(input_path, output_path):
    spark = create_spark_session()
    
    print(f"Reading HTMLs from {input_path}...")
    df = spark.read.format("binaryFile") \
        .option("pathGlobFilter", "*.html") \
        .option("recursiveFileLookup", "true") \
        .load(input_path)
    
    if df.count() == 0:
        print(f"No HTML files found in {input_path}")
        spark.stop()
        return

    print("Extracting Metadata & Text...")
    df = df.withColumn("path_parts", split(col("path"), "/"))
    
    # Extract Ticker from path
    df = df.withColumn("ticker", element_at(col("path_parts"), -4))
    
    # Parsing
    df = df.withColumn("raw_text", parse_html_udf(col("content")))
    df = df.withColumn("cleaned_text", clean_text_udf(col("raw_text")))
    
    # Extract Year
    print("Extracting Years...")
    df = df.withColumn("year", extract_year_udf(col("cleaned_text")))
    
    # Split Sections
    print("Splitting Sections...")
    df = df.withColumn("sections", split_sections_udf(col("cleaned_text")))
    
    # Explode Sections
    df_exploded = df.select(
        col("ticker"),
        col("year"),
        explode(col("sections")).alias("section_struct")
    )
    
    df_final = df_exploded.select(
        col("ticker"),
        col("year"),
        col("section_struct.section_name").alias("section"),
        col("section_struct.content").alias("text")
    )
    
    print(f"Writing partitioned Parquet to {output_path}...")
    # Partition by Ticker AND Year
    df_final.write.mode("overwrite").partitionBy("ticker", "year").parquet(output_path)
    
    print("Ingestion verified. Silver layer created with Sections.")
    spark.stop()

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    INPUT_PATH = os.path.join(BASE_DIR, "data/raw_pdfs") 
    OUTPUT_PATH = os.path.join(BASE_DIR, "data/parquet")
    
    # Fallback to verify we are looking in the right place
    if not os.path.exists(INPUT_PATH):
        if os.path.exists(os.path.join(BASE_DIR, "sec-edgar-filings")):
             INPUT_PATH = os.path.join(BASE_DIR, "sec-edgar-filings")
    
    run_ingestion(INPUT_PATH, OUTPUT_PATH)

