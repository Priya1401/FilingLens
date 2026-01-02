import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import ArrayType, StringType
from langchain_text_splitters import RecursiveCharacterTextSplitter

# Ensure driver and worker use the same python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def create_spark_session():
    spark = SparkSession.builder \
        .appName("FilingLens_Chunking_Gold") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    return spark

def chunk_text(text):
    if not text:
        return []
    
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        separators=["\n\n", "\n", ". ", " ", ""]
    )
    return splitter.split_text(text)

chunk_text_udf = udf(chunk_text, ArrayType(StringType()))

def run_chunking(input_path, output_path):
    spark = create_spark_session()
    
    print(f"Reading Silver Parquet from {input_path}...")
    df = spark.read.parquet(input_path)
    
    print("Chunking Text (Gold Layer)...")
    df_chunked = df.withColumn("chunks", chunk_text_udf(col("cleaned_text")))
    
    # Explode to create one row per chunk (Document -> N Chunks)
    df_exploded = df_chunked.select(
        col("ticker"),
        col("source_path"),
        explode(col("chunks")).alias("chunk_text")
    )
    
    print(f"Writing Gold Parquet to {output_path}...")
    df_exploded.write.mode("overwrite").partitionBy("ticker").parquet(output_path)
    
    print("Chunking complete. Gold layer ready.")
    spark.stop()

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    INPUT_PATH = os.path.join(BASE_DIR, "data/parquet")
    OUTPUT_PATH = os.path.join(BASE_DIR, "data/gold_chunks")
    
    run_chunking(INPUT_PATH, OUTPUT_PATH)
