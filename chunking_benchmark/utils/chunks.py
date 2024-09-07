import numpy as np
import matplotlib.pyplot as plt
import statistics

from chunking_benchmark.utils.tokens import get_num_tokens

def save_chunk_size_distribution(chunks: list[str], title: str, path: str):
  chunk_sizes = [get_num_tokens(chunk) for chunk in chunks]
  plt.hist(chunk_sizes, bins=100)
  plt.xlabel("Chunk Size")
  plt.ylabel("Frequency")
  plt.title(title)
  plt.savefig(path)  
  
  # reset plt
  plt.clf()


def get_chunk_stats(chunks: list[str]):
  chunk_sizes = [get_num_tokens(chunk) for chunk in chunks]

  num_chunks = len(chunks)

  return {
      "avg_chunk_size": np.mean(chunk_sizes) if num_chunks > 0 else 0,
      "max_chunk_size": max(chunk_sizes) if num_chunks > 0 else 0,
      "min_chunk_size": min(chunk_sizes) if num_chunks > 0 else 0,
      "median": statistics.median(chunk_sizes) if num_chunks > 0 else 0,
      "variance": np.var(chunk_sizes) if num_chunks > 0 else 0,
      "std_dev": np.std(chunk_sizes) if num_chunks > 0 else 0,
      "num_chunks": num_chunks,
      "chunk_sizes": chunk_sizes
  }

def print_chunk_stats(chunks: list[str]):
  chunk_sizes = [get_num_tokens(chunk) for chunk in chunks]

  avg_chunk_size = np.mean(chunk_sizes)
  print(f"avg chunk size: {avg_chunk_size}")
  print(f"max chunk size: {max(chunk_sizes)}")
  print(f"min chunk size: {min(chunk_sizes)}")
  print(f"median: {statistics.median(chunk_sizes)}")
  variance = np.var(chunk_sizes)
  print(f"variance: {variance}")
  std_dev = np.std(chunk_sizes)
  print(f"std dev: {std_dev}")
  print(f"num chunks: {len(chunks)}")