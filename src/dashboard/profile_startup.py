
import time
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

print(f"Python executable: {sys.executable}")
print("Starting profile of LAZY LOADED modules...")

# Measure time to import modules that should now be fast
t0 = time.time()
from dotenv import load_dotenv
print(f"Load dotenv: {time.time() - t0:.4f}s")

t0 = time.time()
import src.rag.embeddings
print(f"Import src.rag.embeddings (should be fast): {time.time() - t0:.4f}s")

t0 = time.time()
import src.rag.vector_db
print(f"Import src.rag.vector_db (should be fast): {time.time() - t0:.4f}s")

t0 = time.time()
import src.rag.retrieval_service
print(f"Import src.rag.retrieval_service (should be fast): {time.time() - t0:.4f}s")

print("Imports complete. Now simulating first call (which should trigger lazy load)...")
