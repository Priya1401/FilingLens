import os
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, struct
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
import pdfplumber
from langchain_text_splitters import RecursiveCharacterTextSplitter

def create_spark_session():
    """Initializes a Spark session suitable for local development."""
    spark = SparkSession.builder \
        .appName("FilingLens_Ingestion") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    return spark

# UDF to parse PDF bytes and extract text
def parse_pdf(file_content):
    """
    Extracts text from PDF bytes using pdfplumber.
    Returns a list of page texts.
    """
    try:
        with pdfplumber.open(io.BytesIO(file_content)) as pdf:
            text_content = []
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_content.append(page_text)
            return "\n".join(text_content)
    except Exception as e:
        return f"Error parsing PDF: {str(e)}"

# Register UDF
# Input: Binary (PDF bytes), Output: String (Full Text)
parse_pdf_udf = udf(parse_pdf, StringType())

# UDF to chunk text
def chunk_text(text):
    """
    Splits text into chunks using LangChain's RecursiveCharacterTextSplitter.
    """
    if not text or text.startswith("Error parsing PDF"):
        return []
    
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=100,
        separators=["\n\n", "\n", " ", ""]
    )
    chunks = splitter.split_text(text)
    return chunks

# Register UDF
# Input: String (Full Text), Output: Array of Strings (Chunks)
chunk_text_udf = udf(chunk_text, ArrayType(StringType()))

def run_pipeline(input_path, output_path):
    spark = create_spark_session()
    
    print(f"Reading PDFs from {input_path}...")
    # Read binary files
    df = spark.read.format("binaryFile") \
        .option("pathGlobFilter", "*.pdf") \
        .option("recursiveFileLookup", "true") \
        .load(input_path)
    
    # Extract filename from path
    df = df.withColumn("filename", col("path"))
    
    print("Parsing PDFs...")
    # Parse PDF content
    df_parsed = df.withColumn("full_text", parse_pdf_udf(col("content")))
    
    print("Chunking Text...")
    # Chunk text
    df_chunked = df_parsed.withColumn("chunks", chunk_text_udf(col("full_text")))
    
    # Explode chunks to have one row per chunk
    df_exploded = df_chunked.select(
        col("filename"),
        explode(col("chunks")).alias("chunk_text")
    )
    
    print(f"Writing parsed chunks to {output_path}...")
    df_exploded.write.mode("overwrite").parquet(output_path)
    
    print("Pipeline completed successfully!")
    spark.stop()

if __name__ == "__main__":
    # Define paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    INPUT_PATH = os.path.join(BASE_DIR, "data/raw")
    OUTPUT_PATH = os.path.join(BASE_DIR, "data/processed")
    
    run_pipeline(INPUT_PATH, OUTPUT_PATH)
