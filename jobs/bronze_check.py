"""Verifica el contenido de Bronze: filas, columnas, rango de fechas y cobertura de usuarios.
Uso (con el Python del venv de DLT):  python bronze_check.py
Solo lee las columnas necesarias, asi que es rapido aunque los archivos pesen GB.
"""
import pyarrow.compute as pc
import pyarrow.parquet as pq

import config
from bronze_seed import fs_minio

fs = fs_minio()


def rows_cols(path):
    with fs.open(path, "rb") as f:
        pf = pq.ParquetFile(f)
        return pf.metadata.num_rows, pf.metadata.num_columns, pf.metadata.num_row_groups


authors = {}
for period in sorted(config.PERIODS):
    start, end = config.period_window(period)
    path = config.bronze_path("posts", period)
    if not fs.exists(path):
        print(f"[posts {period}] NO EXISTE: {path}")
        continue
    rows, cols, groups = rows_cols(path)
    with fs.open(path, "rb") as f:
        t = pq.read_table(f, columns=["CreationDate", "OwnerUserId", "PostTypeId"])
    mm = pc.min_max(t["CreationDate"]).as_py()
    ok = start <= mm["min"] and mm["max"] < end
    ids = pc.unique(t["OwnerUserId"])
    authors[period] = len(ids.filter(pc.greater(ids, 0)))
    tipos = {d["values"]: d["counts"] for d in pc.value_counts(t["PostTypeId"]).to_pylist()}
    print(f"[posts {period}] {rows} filas, {cols} columnas, {groups} row groups")
    print(f"    fechas {mm['min']:%Y-%m-%d} .. {mm['max']:%Y-%m-%d} -> dentro del periodo: {'SI' if ok else 'NO !!'}")
    print(f"    autores distintos (OwnerUserId > 0): {authors[period]} | PostTypeId: {dict(sorted(tipos.items()))}")
    print(f"    sin autor (OwnerUserId = 0): {pc.sum(pc.equal(t['OwnerUserId'], 0)).as_py()} filas")

for period in sorted(config.PERIODS):
    path = config.bronze_path("users", period)
    if not fs.exists(path):
        print(f"[users {period}] no cargado todavia")
        continue
    rows, cols, groups = rows_cols(path)
    cob = f"{100 * rows / authors[period]:.1f}%" if authors.get(period) else "n/d"
    print(f"[users {period}] {rows} filas, {cols} columnas | autores de posts: {authors.get(period)} | cobertura: {cob}")