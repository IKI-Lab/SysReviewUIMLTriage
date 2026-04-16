import pandas as pd
from Bio import Entrez
import time
import os
import re

# Set email for NCBI Entrez
Entrez.email = ""

# Set Path for saving the data
PATH = ''

# === Step 1: PubMed Retrieval ===
def fetch_pubmed():
    print("Fetching data from PubMed...")
    query = """
      (
        "machine learning"[tiab] OR
        "artificial intelligence"[tiab] OR
        "ML"[tiab] OR
        "AI"[tiab] OR
        "deep learning"[tiab] OR
        "neural network*"[tiab] OR
        "supervised learning"[tiab] OR
        "reinforcement learning"[tiab] OR
        "support vector machine*"[tiab] OR
        "random forest*"[tiab] OR
        "gradient boosting"[tiab] OR
        "xgboost"[tiab] OR
        "decision tree*"[tiab] OR
        "naive bayes"[tiab] OR
        "k-nearest neighbor*"[tiab] OR
        "classification model*"[tiab] OR
        "prediction model*"[tiab] OR
        "automated model*"[tiab] OR
        "predictive analytics"[tiab] OR
        "computational model*"[tiab] OR
        "data-driven model*"[tiab] OR
        "algorithm*"[tiab] OR
        "Machine Learning"[MeSH Terms] OR
        "Artificial Intelligence"[MeSH Terms] OR
        "Neural Networks, Computer"[MeSH Terms] OR
        "Algorithms"[MeSH Terms]
      )
      AND
      (
        "triage"[tiab] OR 
        "acuity assessment"[tiab] OR 
        "patient prioritization"[tiab] OR 
        "clinical deterioration"[tiab] OR 
        "urgency assessment"[tiab] OR 
        "early warning score"[tiab] OR 
        "triage model"[tiab]
      )
      AND
      (
        "emergency department"[tiab] OR 
        "emergency room"[tiab] OR 
        "accident and emergency"[tiab] OR 
        "emergency care"[tiab] OR 
        "urgent care"[tiab] OR 
        "ED"[tiab] OR
        "ER"[tiab] OR 
        "Emergency Medicine"[MeSH Terms] OR 
        "Emergency Service, Hospital"[MeSH Terms] OR 
        "Emergency Medical Services"[MeSH Terms]
      )
      NOT
      (
        "covid-19"[tiab] OR 
        "sars-cov-2"[tiab] OR 
        "coronavirus"[tiab] OR 
        "pandemic"[tiab] OR 
        "ncov"[tiab] OR 
        "epidemic"[tiab] OR 
        "disaster"[tiab] OR 
        "mass casualty"[tiab] OR 
        "crisis"[tiab] OR 
        "telemonitoring"[tiab] OR 
        "telemedicine"[tiab] OR 
        "remote monitoring"[tiab] OR 
        "virtual care"[tiab] OR 
        "prehospital"[tiab] OR 
        "out-of-hospital"[tiab] OR 
        "general practice"[tiab] OR 
        "primary care"[tiab] OR 
        "GP"[tiab]
      )
      AND ("2015/01/01"[PDAT] : "3000"[PDAT])
      AND english[lang]
      """

    handle = Entrez.esearch(db="pubmed", term=query, retmax=15000)
    record = Entrez.read(handle)
    handle.close()

    id_list = record["IdList"]
    if not id_list:
        return pd.DataFrame()

    time.sleep(1)  # respectful delay
    handle = Entrez.efetch(db="pubmed", id=",".join(id_list), rettype="abstract", retmode="xml")
    papers = Entrez.read(handle)
    handle.close()

    results = []

    for article in papers["PubmedArticle"]:
        title = article["MedlineCitation"]["Article"].get("ArticleTitle", "")
        abstract = article["MedlineCitation"]["Article"].get("Abstract", {}).get("AbstractText", [""])[0]
        pmid = article["MedlineCitation"]["PMID"]

        # Extract DOI if available
        doi = ""
        elocation_list = article["MedlineCitation"]["Article"].get("ELocationID", [])
        for eloc in elocation_list:
            if eloc.attributes.get("EIdType") == "doi":
                doi = str(eloc)
                break

        # Extract Authors
        authors = []
        author_list = article["MedlineCitation"]["Article"].get("AuthorList", [])
        for author in author_list:
            lastname = author.get("LastName", "")
            firstname = author.get("ForeName", "")
            fullname = f"{firstname} {lastname}".strip()
            if fullname:
                authors.append(fullname)
        authors_str = "; ".join(authors)


        results.append({
            "Title": title,
            "Abstract": abstract,
            "PMID": str(pmid),
            "DOI": doi,
            "Authors": authors_str
        })
    pd.DataFrame(results).to_csv("pubmed_data.csv", index=False)
    return pd.DataFrame(results)


def deduplicate(df):
    print(f"\n🔍 Deduplicating {len(df)} records...")

    # Normalize title and DOI columns
    df['normalized_title'] = df['Title'].str.lower().str.strip()
    if 'DOI' in df.columns:
        df['normalized_doi'] = df['DOI'].str.lower().str.strip()
    else:
        df['normalized_doi'] = None

    # Step 1: Drop exact DOI duplicates (if DOI exists)
    df_dedup = df.drop_duplicates(subset='normalized_doi', keep='first') if df['normalized_doi'].notna().any() else df

    # Step 2: Drop by title if DOI is missing or duplicate
    df_dedup = df_dedup.drop_duplicates(subset='normalized_title', keep='first')

    print(f"{len(df_dedup)} records remain after deduplication.")
    return df_dedup.drop(columns=['normalized_title', 'normalized_doi'])

def deduplicate_spec(df):
    print(f"\n🔍 Deduplicating {len(df)} records...")

    def normalize_text(text):
        if pd.isna(text):
            return ""
        # Lowercase
        text = text.lower().strip()
        # Remove double quotes and numbers
        text = re.sub(r'["0-9]', '', text)
        # Collapse multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    # Normalize title and DOI columns
    df['normalized_title'] = df['Title'].astype(str).apply(normalize_text)
    if 'DOI' in df.columns:
        df['normalized_doi'] = df['DOI'].astype(str).apply(normalize_text)
    else:
        df['normalized_doi'] = None

    # Step 1: Drop exact DOI duplicates (if DOI exists)
    if df['normalized_doi'].notna().any():
        df_dedup = df.drop_duplicates(subset='normalized_doi', keep='first')
    else:
        df_dedup = df

    # Step 2: Drop by normalized title
    df_dedup = df_dedup.drop_duplicates(subset='normalized_title', keep='first')

    print(f"{len(df_dedup)} records remain after deduplication.")
    return df_dedup.drop(columns=['normalized_title', 'normalized_doi'])

pubmed_df = pd.read_csv(PATH+'pubmed_data.csv', sep=',') #fetch_pubmed()
embase_df = pd.read_csv(PATH+'records.csv', sep=',')
central_df = pd.read_csv(PATH+'citation-export.csv', sep=',')
ieee_df = pd.read_csv(PATH+'export2025.08.19-07.17.49.csv', sep=',')
webscience_df = pd.read_csv(PATH+'savedrecs.csv', sep=';', encoding='latin')

pubmed_df['data_base'] = 'pubmed'
embase_df['data_base'] = 'embase'
central_df['data_base'] = 'central'
ieee_df['data_base'] = 'ieee'
webscience_df['data_base'] = 'webscience'

# Align column namesa
ieee_df = ieee_df.rename(columns={'Document Title': 'Title'})
embase_df = embase_df.rename(columns={'Author Names': 'Authors'})
webscience_df = webscience_df.rename(columns={'Article Title': 'Title'})
central_df = central_df.rename(columns={'Author(s)': 'Authors'})
print([len(df) for df in [pubmed_df, embase_df, central_df, ieee_df, webscience_df]])

all_dfs = [df[['Title', 'Abstract', 'DOI', 'Authors', 'data_base']] for df in
           [pubmed_df, embase_df, central_df, ieee_df, webscience_df] if not df.empty]
combined_df = pd.concat(all_dfs, ignore_index=True)
final_df = deduplicate(combined_df)
print(len(final_df))

final_df.to_csv(PATH+"combined_deduplicated_results.csv", sep=';', index=False)
print("\n Saved: combined_deduplicated_results.csv")
