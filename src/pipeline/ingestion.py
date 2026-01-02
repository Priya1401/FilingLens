import os
import io
import re
import sys
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, element_at, split
from pyspark.sql.types import StringType

# Ensure driver and worker use the same python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def create_spark_session():
    """Initializes a Spark session suitable for local development."""
    spark = SparkSession.builder \
        .appName("FilingLens_Ingestion_Silver") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
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

def run_ingestion(input_path, output_path):
    spark = create_spark_session()
    
    print(f"Reading HTMLs from {input_path}...")
    # Change filter to *.html
    df = spark.read.format("binaryFile") \
        .option("pathGlobFilter", "*.html") \
        .option("recursiveFileLookup", "true") \
        .load(input_path)
    
    if df.count() == 0:
        print(f"No HTML files found in {input_path}")
        spark.stop()
        return

    print("Extracting Metadata...")
    df = df.withColumn("path_parts", split(col("path"), "/"))
    
    # Extract Ticker from path
    # Path: .../sec-edgar-filings/TICKER/10-K/...
    # element -4 is usually Ticker in this structure
    df = df.withColumn("ticker", element_at(col("path_parts"), -4))
    
    print("Parsing HTMLs...")
    df_parsed = df.withColumn("raw_text", parse_html_udf(col("content")))
    
    print("Cleaning Text...")
    df_cleaned = df_parsed.withColumn("cleaned_text", clean_text_udf(col("raw_text")))
    
    # Select final columns
    df_final = df_cleaned.select(
        col("ticker"),
        col("path").alias("source_path"),
        col("cleaned_text")
    )
    
    print(f"Writing Parquet to {output_path}...")
    df_final.write.mode("overwrite").partitionBy("ticker").parquet(output_path)
    
    print("Ingestion verified. Silver layer created.")
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
