from transformers import AutoTokenizer
import tiktoken

# global_tokenizer = AutoTokenizer.from_pretrained("baseten/Meta-Llama-3-tokenizer")
global_tokenizer = tiktoken.get_encoding("o200k_base")

def get_num_tokens(text: str):
  return len(global_tokenizer.encode(text))

def get_tokens_from_text(text: str):
  return global_tokenizer.encode(text)

def get_text_from_tokens(tokens: list[int]):
  return global_tokenizer.decode(tokens)