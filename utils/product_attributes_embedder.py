from sentence_transformers import SentenceTransformer

# Initialize embedding model
embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

def get_text_embeddings(text):
    embedding = embedding_model.encode(text)
    return embedding