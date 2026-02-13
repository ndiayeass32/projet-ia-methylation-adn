import os
import numpy as np
import pandas as pd

# =========================
# PARAMÈTRES
# =========================
ANNOT_PATH = "annot_projet.csv"
BETA_PATH  = "GSE246337_beta_projet.csv"
CPG_PATH   = "cpg_names_projet.csv"

SEP = ","                  # si c'est un .tsv -> "\t"
CHUNKSIZE = 10000        # 2000 CpG par bloc (augmente si RAM OK)
OUT_DIR = "wide_parquet"   # dossier de sortie (dataset parquet)

os.makedirs(OUT_DIR, exist_ok=True)

# =========================
# 1) ANNOT + SUJETS
# =========================
annot = pd.read_csv(ANNOT_PATH)
annot.columns = annot.columns.astype(str).str.strip()

subjects_col = "Sample_description"
subjects = annot[subjects_col].astype(str).tolist()

# covariables + cible
annot2 = annot.set_index(subjects_col)[["age", "female", "ethnicity"]].copy()

print("Nb sujets (annot):", len(subjects))
print("Exemples sujets:", subjects[:5])

# =========================
# 2) LIRE HEADER BETA (colonnes = sujets)
# =========================
header = pd.read_csv(BETA_PATH, sep=SEP, nrows=0)
header.columns = header.columns.astype(str).str.strip()

missing = [s for s in subjects if s not in header.columns]
print("Sujets manquants dans beta:", len(missing))
if missing:
    print("Exemples manquants:", missing[:10])
    raise ValueError("Mismatch entre annot (Sample_description) et colonnes du fichier beta.")

# on lit uniquement les colonnes sujets (pas de colonne CpG ici)
usecols = subjects

# =========================
# 3) LIRE LISTE CpG + SUPPRIMER LE 1er ELEMENT SI C'EST 'cpg_names'
# =========================
cpg_series = pd.read_csv(CPG_PATH, header=None).iloc[:, 0].astype(str)

# 🔥 enlever un header parasite s'il existe
first_value = cpg_series.iloc[0].strip().lower()
if first_value in ["cpg_names", "cpg", "name", "names"]:
    print("⚠️ Header CpG détecté et supprimé :", cpg_series.iloc[0])
    cpg_series = cpg_series.iloc[1:].reset_index(drop=True)

cpg_list = cpg_series.tolist()

print("Nb CpG (corrigé):", len(cpg_list))
print("Exemples CpG:", cpg_list[:5])

# =========================
# 4) STREAMING : lire beta par chunks + écrire parquet LARGE (partitions)
# =========================
print("\nCréation parquet LARGE partitionné...")

pos = 0
part = 0

for chunk in pd.read_csv(BETA_PATH, sep=SEP, usecols=usecols, chunksize=CHUNKSIZE):
    n = len(chunk)

    # associer les CpG à ce bloc
    if pos + n > len(cpg_list):
        raise ValueError(
            f"Le fichier beta a plus de lignes ({pos+n}) que cpg_names_projet ({len(cpg_list)})."
        )

    cpg_ids = cpg_list[pos:pos + n]
    pos += n

    # chunk = CpG_chunk x sujets (sans index) -> on met l'index CpG
    chunk.index = cpg_ids

    # conversion numérique
    chunk = chunk.apply(pd.to_numeric, errors="coerce").astype("float32")

    # LARGE : sujets x CpG_chunk
    wide = chunk.T
    wide.index = wide.index.astype(str)
    wide.index.name = "sample"

    # ajout covariables
    wide = wide.join(annot2, how="inner")

    out_path = os.path.join(OUT_DIR, f"part_{part:04d}.parquet")
    wide.to_parquet(out_path, engine="pyarrow", index=True)

    part += 1
    if part % 10 == 0:
        print(f"Parts écrites: {part} | CpG traités: {pos}")

# =========================
# 5) CHECK FIN
# =========================
if pos != len(cpg_list):
    print(f"⚠️ Attention: CpG restants non utilisés: {len(cpg_list) - pos}")
else:
    print("✅ Tous les CpG ont été utilisés (alignement OK)")

print("\n✅ Terminé.")
print("Dossier créé:", os.path.abspath(OUT_DIR))
print("Exemple fichier:", os.path.join(os.path.abspath(OUT_DIR), "part_0000.parquet"))
