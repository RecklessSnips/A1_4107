import pandas as pd
import json
import csv
from sentence_transformers import SentenceTransformer, util
from tqdm import tqdm

# Model
model = SentenceTransformer("all-MiniLM-L6-v2")
run_name = "bert-v1"

# Paths
csv_path = "data/a1.csv"
query_path = "data/queries.jsonl.json"
corpus_path = "data/corpus.jsonl.json"
output_path = "data/results.csv"

# Read the queries
query_map = {}
with open(query_path, "r", encoding="utf-8") as f:
    for line in f:
        # Interpret as a JSON object
        obj = json.loads(line)
        # Map the id and the text
        query_map[str(obj["_id"])] = obj["text"]

# Read the corpus
doc_map = {}
with open(corpus_path, "r", encoding="utf-8") as f:
    for line in f:
        obj = json.loads(line)
        # Combine the title and the text field as a single string
        content = f"{obj.get('title', '')}. {obj.get('text', '')}"
        doc_map[str(obj["_id"])] = content

# Read results.csv
columns = ["query_id", "Q0", "doc_id", "rank", "score", "tag"]
rows = []
with open(csv_path, "r", encoding="utf-8") as f:
    # Split the CSV file
    reader = csv.reader(f, delimiter="\t")
    for row in reader:
        # Neglect incomplete rows
        if len(row) >= 6:
            rows.append(row)
# Jump over the header, and using the columns I specified above
df = pd.DataFrame(rows[1:], columns=columns)

# Data cleaning
df["query_id"] = df["query_id"].astype(str)
df["doc_id"] = df["doc_id"].str.replace('"', '')

# Prepare the output
results_output = []

# Iterate the df
for query_id, group in tqdm(df.groupby("query_id")):
    # Get the query from the query_map based on the current query_id
    query_text = query_map.get(query_id)
    if not query_text:
        continue

    # Get doc_id from the top 100 docs
    doc_ids = group["doc_id"].tolist()
    # Find the "title + text" field in the doc_map based on the top 100 related docs
    doc_texts = [doc_map.get(doc_id, "") for doc_id in doc_ids]

    # Calculating the embeddings (vectors), something like ([0.12, -0.04, 0.53, ..., 0.98])
    # Convert to PyTorch Tensor for similarity computaion
    query_emb = model.encode(query_text, convert_to_tensor=True)
    doc_embs = model.encode(doc_texts, convert_to_tensor=True)

    # Calculate the similarity for the current query
    # The shape for query_emb is [1, D], so we take the first row
    scores = util.cos_sim(query_emb, doc_embs)[0]
    # Sort the scores
    sorted_docs = sorted(zip(doc_ids, scores), key=lambda x: x[1], reverse=True)

    # After we get the sorted scores
    for rank, (doc_id, score) in enumerate(sorted_docs, start=1):
        # Using these columns
        # And change the PyTorch tensor into python floating point
        # Keep 4 decimal places
        results_output.append([query_id, "Q0", doc_id, rank, f"{score.item():.4f}", run_name])

# Write into the result file!
results_df = pd.DataFrame(results_output, columns=["query_id", "Q0", "doc_id", "rank", "score", "run_name"])
results_df.to_csv(output_path, index=False)
print(f"Results saved to: {output_path}")
