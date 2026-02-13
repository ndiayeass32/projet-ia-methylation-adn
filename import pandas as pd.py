import pandas as pd

import pandas as pd

# Charger ton fichier
df = pd.read_csv("GSE246337_beta_projet.csv")

# Nombre de lignes du dataset
n = len(df)

print("Nombre de lignes :", n)

# Créer la colonne CpG
df.insert(0, "CpG_ID", ["cg" + str(i) for i in range(1, n + 1)])

# Sauvegarder le nouveau fichier
df.to_csv("data_with_cpg.csv", index=False)

print("Colonne CpG ajoutée avec succès.")