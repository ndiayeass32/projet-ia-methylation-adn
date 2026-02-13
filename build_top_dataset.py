import os
import pandas as pd
import numpy as np

DATA_DIR = "wide_parquet"
TOP_FILE = "top_cpg_10000.csv"

# Charger liste top CpG
top_df = pd.read_csv(TOP_FILE)
top_cpg = set(top_df["cpg"].tolist())

print("Nb CpG sélectionnés:", len(top_cpg))

X_parts = []

for file in sorted(os.listdir(DATA_DIR)):
    if file.endswith(".parquet"):
        path = os.path.join(DATA_DIR, file)
        df = pd.read_parquet(path)

        # garder uniquement CpG présents dans top list
        cols = [c for c in df.columns if c in top_cpg]
        
        if len(cols) > 0:
            X_parts.append(df[cols])

        print(file, "checked")

# concat horizontal
X_final = pd.concat(X_parts, axis=1)

# enlever doublons éventuels
X_final = X_final.loc[:, ~X_final.columns.duplicated()]

print("Shape final X:", X_final.shape)

# ajouter age
df_any = pd.read_parquet(os.path.join(DATA_DIR, "part_0000.parquet"))
y = df_any["age"]

data_final = X_final.copy()
data_final["age"] = y

data_final.to_parquet("dataset_top10000.parquet")

print("\nDataset final sauvegardé.")
print("Shape:", data_final.shape)
