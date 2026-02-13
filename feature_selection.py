import os
import pandas as pd
import numpy as np

DATA_DIR = "wide_parquet"

top_k = 10000   # on vise 10 000 CpG

all_scores = []

print("Calcul corrélations...")

for file in sorted(os.listdir(DATA_DIR)):
    if file.endswith(".parquet"):
        path = os.path.join(DATA_DIR, file)
        df = pd.read_parquet(path)

        y = df["age"].astype(float)
        X = df.drop(columns=["age", "female", "ethnicity"])

        # corrélation absolue CpG ↔ âge
        corrs = X.apply(lambda col: np.corrcoef(col, y)[0,1])

        tmp = pd.DataFrame({
            "cpg": corrs.index,
            "score": corrs.abs().values
        })

        all_scores.append(tmp)

        print(file, "done")

# concat tous les scores
scores_df = pd.concat(all_scores)

# garder top_k
top_features = scores_df.sort_values("score", ascending=False).head(top_k)

top_features.to_csv("top_cpg_10000.csv", index=False)

print("\nTop CpG sauvegardés :", len(top_features))
