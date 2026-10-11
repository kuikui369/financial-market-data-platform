import psycopg2
from sentence_transformers import SentenceTransformer

# 1. Load the lightweight 384-dimensional dense vector model
model = SentenceTransformer('all-MiniLM-L6-v2')

# 2. Connect to the local PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="market_ledger",
    user="quant_admin",
    password="quant_password"
)
cursor = conn.cursor()

# 3. Seed synthetic financial headlines
headlines = [
    ("AAPL", "Apple announces record-breaking iPhone sales driven by AI integration"),
    ("TSLA", "Tesla vehicle deliveries drop amidst growing EV competition"),
    ("NVDA", "NVIDIA unveils next-generation Blackwell architecture for hyperscalers"),
    ("AAPL", "Foxconn boosts factory production schedules for next Apple devices")
]

print("Indexing market news headlines into pgvector...")
for ticker, text in headlines:
    vector = model.encode(text).tolist()
    cursor.execute(
        "INSERT INTO market_news_rag (ticker, headline, embedding) VALUES (%s, %s, %s);",
        (ticker, text, str(vector))
    )
conn.commit()

# 4. Perform Cosine Similarity Semantic Search using the <=> operator
query = "artificial intelligence semiconductor growth"
query_vector = model.encode(query).tolist()

cursor.execute("""
    SELECT ticker, headline, 1 - (embedding <=> %s::vector) AS cosine_similarity
    FROM market_news_rag
    ORDER BY embedding <=> %s::vector
    LIMIT 2;
""", (str(query_vector), str(query_vector)))

results = cursor.fetchall()
print(f"\nSemantic Query: '{query}'")
print("-" * 75)
for ticker, text, sim in results:
    print(f"[{ticker}] Similarity: {sim:.4f} | Headline: {text}")

cursor.close()
conn.close()