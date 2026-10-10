"""Lista todo lo que hay en el bucket bronze (para comprobar que repetir el seed no crea rutas nuevas).
Uso: python bronze_ls.py
"""
import config
from bronze_seed import fs_minio

fs = fs_minio()
paths = sorted(fs.find(config.BRONZE_BUCKET))
for p in paths:
    print(f"{fs.size(p) / 1e6:10.1f} MB  {p}")
print(f"{len(paths)} archivos")
