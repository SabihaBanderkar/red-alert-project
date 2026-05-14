import pandas as pd
import numpy as np

# load data

clean_df = pd.read_csv(
    "reddit_cleaned_full.csv"
)

# datetime conversion
clean_df['created_utc'] = pd.to_datetime(
    clean_df['created_utc'],
    errors='coerce'
)

print(" Cleaned data loaded!")

print("\nColumns:\n")

print(clean_df.columns.tolist())

clean_df = clean_df.reset_index(drop=True)



# ---------------------------
# SAFE Z-SCORE FUNCTION
# ---------------------------
def safe_zscore(x):
    mean = x.mean()
    std = x.std()

    if std == 0 or pd.isna(std):
        return pd.Series([0]*len(x), index=x.index)

    return (x - mean) / std


# ---------------------------
# CALCULATE Z-SCORE
# ---------------------------
clean_df['z_score'] = clean_df.groupby('subreddit')['score'].transform(safe_zscore)


# ---------------------------
# BASELINE TABLE (optional)
# ---------------------------
baseline = clean_df.groupby('subreddit')['score'].agg(['mean', 'std']).reset_index()
baseline.columns = ['subreddit', 'mean_score', 'std_score']
baseline.to_csv("baseline_table.csv", index=False)



# ---------------------------
# SUBREDDIT-WISE FILTERS
# ---------------------------

# top 5% z-score
z_filter = clean_df.groupby('subreddit')['z_score'].transform(
    lambda x: x > x.quantile(0.95)
)

# top 10% velocity
velocity_filter = clean_df.groupby('subreddit')['engagement_velocity'].transform(
    lambda x: x > x.quantile(0.90)
)

# top 25% comments
comment_filter = clean_df.groupby('subreddit')['num_comments'].transform(
    lambda x: x > x.quantile(0.75)
)

# above median score
score_filter = clean_df.groupby('subreddit')['score'].transform(
    lambda x: x > x.quantile(0.50)
)


# ---------------------------
# FINAL ALERTS
# ---------------------------
alerts = clean_df[
    z_filter &
    velocity_filter &
    comment_filter &
    score_filter
]

alerts = alerts.sort_values(by='z_score', ascending=False)



# ---------------------------
# OUTPUT
# ---------------------------
print("Total alerts:", len(alerts))

print(alerts[['post_id','title','clean_text','subreddit','score','z_score','engagement_velocity','created_utc',
    'created_day',
    'created_hour',
    'post_age_hours']].head())


# ---------------------------
# SAVE FILES
# ---------------------------
clean_df.to_excel("analysis_cleaned_with_zscore.xlsx", index=False)
alerts.to_excel("analysis_alerts.xlsx", index=False)

clean_df = clean_df.reset_index(drop=True)

import requests
import pandas as pd
import time
import os
import re   

# ---------------------------
# CONFIG
# ---------------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Reddit Comment Extractor)"
}

CACHE_FILE = "comments.csv"
RECENT_HOURS = 48   # only for refresh logic


# ---------------------------
# SAFE REQUEST
# ---------------------------
def safe_request(url, retries=5):
    for attempt in range(retries):
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)

            if res.status_code == 429:
                wait = (attempt + 1) * 5
                print(f" Rate limit → waiting {wait}s")
                time.sleep(wait)
                continue

            if res.status_code != 200:
                print(f" Status {res.status_code}")
                time.sleep(3)
                continue

            return res.json()

        except Exception as e:
            print(f" Error: {e}")
            time.sleep(3)

    return None


# ---------------------------
# FETCH COMMENTS (TOP BY SCORE)
# ---------------------------
def fetch_comments(posts_df, max_comments=15):
    all_comments = []

    for _, row in posts_df.iterrows():
        post_id = row['post_id']
        subreddit = row['subreddit']

        url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json?limit=100"
        data = safe_request(url)

        if not data or len(data) < 2:
            print(f" Skipped {post_id}")
            continue

        try:
            comments = data[1]['data']['children']
        except:
            print(f" Structure issue {post_id}")
            continue

        valid_comments = []

        for c in comments:
            if c.get('kind') != 't1':
                continue

            comment = c.get('data', {})
            body = comment.get('body')
            score = comment.get('score', 0)

            if not body or body in ["[deleted]", "[removed]"]:
                continue

            valid_comments.append({
                "post_id": post_id,
                "subreddit": subreddit,
                "comment_text": body,
                "comment_score": score
            })

        #  SORT BY SCORE
        valid_comments = sorted(valid_comments, key=lambda x: x['comment_score'], reverse=True)

        #  TAKE TOP COMMENTS
        top_comments = valid_comments[:max_comments]

        all_comments.extend(top_comments)

        print(f" {post_id}: {len(top_comments)} top comments")
        time.sleep(2)

    return pd.DataFrame(all_comments)



# MAIN PIPELINE (CACHE + REFRESH)


def get_comments_pipeline(alerts_df):
    alerts_df = alerts_df.copy()
    existing_comments = pd.DataFrame()
    fetched_ids = set()
    
    # Load cache
    if os.path.exists(CACHE_FILE):
        existing_comments = pd.read_csv(CACHE_FILE)
        

    # safe check
    if 'post_id' in existing_comments.columns:

        fetched_ids = set(
            existing_comments['post_id'].unique()
        )

    else:

        print(" Invalid cache file")

        existing_comments = pd.DataFrame()

        fetched_ids = set()
    

    # Time calculation
    alerts_df['created_utc'] = pd.to_datetime(alerts_df['created_utc'])
    now = pd.Timestamp.now()

    alerts_df['age_hours'] = (
        (now - alerts_df['created_utc']).dt.total_seconds() / 3600
    )

    
    # LOGIC (ONLY ALERTS USED)
    

    # New posts (not fetched before)
    new_posts = alerts_df[~alerts_df['post_id'].isin(fetched_ids)]

    # Recent posts (refresh)
    recent_posts = alerts_df[alerts_df['age_hours'] <= RECENT_HOURS]

    # FINAL FETCH SET
    posts_to_fetch = pd.concat([new_posts, recent_posts]).drop_duplicates(subset=['post_id'])

    print(f" Fetching comments for {len(posts_to_fetch)} posts")

    # Fetch
    new_comments = fetch_comments(posts_to_fetch)

    # Remove old entries for refreshed posts
    if 'post_id' in existing_comments.columns:

        existing_comments = existing_comments[
        ~existing_comments['post_id']
        .isin(posts_to_fetch['post_id'])
        ]

    # Combine
    comments_df = pd.concat([existing_comments, new_comments], ignore_index=True)

    # Remove duplicates
    comments_df = comments_df.drop_duplicates(subset=['post_id', 'comment_text'])

    # Save cache
    comments_df.to_csv(CACHE_FILE, index=False)

    return comments_df


# ---------------------------
# COMMENT METRICS
# ---------------------------
def compute_comment_metrics(comments_df):
    metrics = comments_df.groupby('post_id').agg({
        'comment_text': 'count',
        'comment_score': ['mean', 'max']
    })

    metrics.columns = ['comment_count', 'avg_comment_score', 'max_comment_score']
    metrics = metrics.reset_index()

    return metrics


# ---------------------------
# CLEANING FUNCTION
# ---------------------------
def clean_comment(text):
    text = str(text)
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"[^a-zA-Z0-9\s\.\,\?\!%₹$]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------
# FULL RUN
# ---------------------------
if __name__ == "__main__":



    # Step 1: Fetch comments
    comments_df = get_comments_pipeline(alerts)

    # Step 2: Clean comments
    comments_df['clean_comment'] = comments_df['comment_text'].apply(clean_comment)

    print("Total comments:", len(comments_df))

    # Save cleaned comments
    comments_df.to_csv("comments_with_clean.csv", index=False)

#  NEW PART 
metrics_df = compute_comment_metrics(comments_df)

# Save metrics separately
metrics_df.to_csv("comment_metrics.csv", index=False)

print(" Comment metrics saved!")
combined_comments = (
    comments_df
    .dropna(subset=['clean_comment'])
    .groupby('post_id')
    .agg(
        all_comments=('clean_comment', lambda x: " ".join(x)),
        comment_count=('clean_comment', 'count')
    )
    .reset_index()
)

combined_comments.to_csv("combined_comments.csv", index=False)

combined_comments.rename(columns={'clean_comment': 'all_comments'}, inplace=True)

combined_comments.to_csv("combined_comments.csv", index=False)



# DEBUG / ANALYSIS

print("\nZ-score summary:")
print(clean_df['z_score'].describe())

print("\nZ-score percentiles:")
print(clean_df['z_score'].quantile([0.90, 0.95, 0.99]))



# SUBREDDIT INFO

subreddits = clean_df['subreddit'].unique()

print("\nTotal subreddits:", len(subreddits))
print(subreddits)


import os
from sentence_transformers import SentenceTransformer


# USE ALERTS ONLY

df_embed = alerts.copy().reset_index(drop=True)

file_name = "embeddings_alerts.npy"

embeddings = None


# LOAD IF EXISTS

if os.path.exists(file_name):
    print("Loading embeddings...")
    embeddings = np.load(file_name)

    if len(embeddings) != len(df_embed):
        print("Mismatch detected → recreating embeddings")
        os.remove(file_name)
        embeddings = None


# CREATE EMBEDDINGS

if embeddings is None:
    print("Creating embeddings...")

    model = SentenceTransformer('all-MiniLM-L6-v2')

    texts = df_embed['clean_text'].tolist()

    embeddings = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=True
    )

    embeddings = np.array(embeddings)

    np.save(file_name, embeddings)
    print("Embeddings saved!")

print("Embedding shape:", embeddings.shape)
print("Number of posts:", len(df_embed))


assert len(alerts) == len(embeddings), "Mismatch between data and embeddings!"


from sklearn.metrics.pairwise import cosine_similarity

sample_texts = df_embed['clean_text'].iloc[:5].tolist()
sample_embeddings = embeddings[:5]

print("\nSample Posts:\n")

for i, text in enumerate(sample_texts):
    print(f"{i}: {text}\n")

similarity_matrix = cosine_similarity(sample_embeddings)
print("Similarity Matrix:\n", similarity_matrix)


import umap.umap_ as umap

print("Running UMAP (384D → 15D)...")

umap_model = umap.UMAP(
    n_components=15,      # output dimensions
    n_neighbors=15,       # local structure
    min_dist=0.1,         # cluster tightness
    metric='cosine',      # VERY IMPORTANT for text
    random_state=42
)

embeddings_15d = umap_model.fit_transform(embeddings)

print("UMAP output shape:", embeddings_15d.shape)



# HDBSCAN CLUSTERING

import hdbscan

print("Running HDBSCAN clustering...")

clusterer = hdbscan.HDBSCAN(
    min_cluster_size=10,
    min_samples=5,
    metric='euclidean'
)

clusters = clusterer.fit_predict(embeddings_15d)

df_embed['cluster'] = clusters

print("\nCluster distribution:")
print(df_embed['cluster'].value_counts())


# SAMPLE POSTS PER CLUSTER

for c in sorted(df_embed['cluster'].unique()):
    print(f"\n--- Cluster {c} ---")
    print(df_embed[df_embed['cluster'] == c]['clean_text'].head(3))

df_embed.to_csv("clustered_posts.csv", index=False)

print(" Clustered data saved!")

#Silhouette Score
from sklearn.metrics import silhouette_score

# remove noise points
mask = clusters != -1

score = silhouette_score(
    embeddings_15d[mask],
    clusters[mask]
)

print("\nSilhouette Score:", round(score, 3))

from sklearn.metrics import silhouette_samples


# CLUSTER-WISE SILHOUETTE


# remove noise points
mask = clusters != -1

filtered_embeddings = embeddings_15d[mask]
filtered_clusters = clusters[mask]

# silhouette score for every point
sample_scores = silhouette_samples(
    filtered_embeddings,
    filtered_clusters
)

# create dataframe
sil_df = pd.DataFrame({
    'cluster': filtered_clusters,
    'silhouette_score': sample_scores
})

# average silhouette per cluster
cluster_silhouette = (
    sil_df
    .groupby('cluster')['silhouette_score']
    .mean()
    .reset_index()
)

print("\nCluster-wise Silhouette Scores:")
print(cluster_silhouette)

cluster_silhouette.to_csv(
    "cluster_silhouette_scores.csv",
    index=False
)

print(" Cluster silhouette scores saved!")



# TREND ANALYSIS

df_embed['created_utc'] = pd.to_datetime(df_embed['created_utc'])

trend = df_embed.groupby('cluster').agg({
    'clean_text': 'count',
    'score': 'mean',
    'num_comments': 'mean',
    'engagement_velocity': 'mean',
    'created_utc': 'max'
})

trend.columns = ['volume', 'avg_score', 'avg_comments', 'avg_velocity', 'latest_post']

trend = trend.sort_values(by='volume', ascending=False)

print("\n TREND SUMMARY:")
print(trend)



# CLUSTER LABELING


from keybert import KeyBERT

print("\nGenerating dynamic cluster labels...")


# LOAD KEYBERT MODEL


kw_model = KeyBERT()


# LABEL STORAGE


cluster_labels = {}


# GENERIC WORD FILTER


BAD_WORDS = [

    'people',
    'work',
    'post',
    'reddit',
    'time',
    'good',
    'better',
    'need',
    'question',
    'advice',
    'help',
    'using'
]


# REMOVE NOISE CLUSTER


valid_clusters = [

    c for c in df_embed['cluster'].unique()

    if c != -1
]


# PROCESS EACH CLUSTER


for c in sorted(valid_clusters):

    print(f"\nProcessing Cluster {c}...")

    
    # POSTS INSIDE CLUSTER
    
    cluster_posts = df_embed[
        df_embed['cluster'] == c
    ]

    
    # COMBINE TEXT
    

    combined_text = " ".join(

        cluster_posts['clean_text']
        .dropna()
        .astype(str)
    )

    # skip tiny clusters
    if len(combined_text.split()) < 20:

        cluster_labels[c] = "miscellaneous"

        continue

    
    # EXTRACT KEYWORDS
    

    keywords = kw_model.extract_keywords(

        combined_text,

        keyphrase_ngram_range=(1,2),

        stop_words='english',

        top_n=10,

        use_mmr=True,

        diversity=0.7
    )

    
    # CLEAN KEYWORDS
    

    final_keywords = []

    for kw, score in keywords:

        kw = kw.lower().strip()

        if kw not in BAD_WORDS:

            final_keywords.append(kw)

    # remove duplicates
    final_keywords = list(dict.fromkeys(final_keywords))

    
    # CREATE LABEL
    
    # take top keyword
    top_keywords = final_keywords[:1]

    if len(top_keywords) == 0:
        label = "miscellaneous"

    else:
        label = " • ".join(top_keywords)
    cluster_labels[c] = label

    print(f"Cluster {c} → {label}")


# MAP LABELS TO DATAFRAME


df_embed['cluster_label'] = (

    df_embed['cluster']
    .map(cluster_labels)
)

# noise points
df_embed['cluster_label'] = (

    df_embed['cluster_label']
    .fillna("noise")
)


df_embed.to_csv(
    "clustered_posts_with_labels.csv",
    index=False
)

print("\n Cluster labels generated!")

print(" Final clustered dataset saved!")