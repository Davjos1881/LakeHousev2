"""Seed manual de Bronze: baja del dataset publico las filas del periodo (con TODAS las columnas)
y las guarda como Parquet en MinIO, en una ruta fija por dataset y periodo (sobrescribe).

Uso (con el Python del venv de DLT, que trae pyarrow y s3fs):
    python bronze_seed.py posts 2020_H1
    python bronze_seed.py posts 2021_H1
    python bronze_seed.py users 2020_H1    # requiere haber cargado antes posts del mismo periodo

Decisiones:
- Bronze es dato crudo: se conservan TODAS las columnas de la fuente.
- ventana de fechas del periodo (ene-jun).
- users es un archivo unico sin periodos, se define el periodo como "autores de los posts del periodo".
- Ceros y textos vacios se dejan tal cual la limpieza es de Silver.
- Lectura en lotes de para acotar la memoria (Body es muy pesado).
"""
import resource
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import s3fs

import config

BATCH_ROWS = 300_000
LOG_EVERY = 1  # lotes entre mensajes de progreso 


# ---------- sistemas de archivos ----------
def fs_source():
    return s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": config.SOURCE_REGION})


def fs_minio():
    return s3fs.S3FileSystem(
        key=config.MINIO_KEY,
        secret=config.MINIO_SECRET,
        client_kwargs={"endpoint_url": config.MINIO_ENDPOINT},
        config_kwargs={"s3": {"addressing_style": "path"}},
    )


# ---------- filtro de filas ----------
def window_filter(table, start, end):
    ts = pa.timestamp("ms", tz="UTC")
    s, e = pa.scalar(start, type=ts), pa.scalar(end, type=ts)
    mask = pc.and_(pc.greater_equal(table["CreationDate"], s), pc.less(table["CreationDate"], e))
    return table.filter(mask)


def peak_mb():
    """Pico de memoria residente del proceso (Linux: ru_maxrss viene en KB)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


# ---------- lectura en streaming (lotes pequenos, memoria acotada) ----------
def stream_filtered(fs, path, predicate, batch_rows=BATCH_ROWS):
    with fs.open(path, "rb") as f:
        pf = pq.ParquetFile(f, pre_buffer=True)
        total = pf.metadata.num_rows
        print(f"  {total} filas en origen, {pf.num_row_groups} row groups; lotes de {batch_rows}", flush=True)
        t0 = time.time()
        read = kept = 0
        for i, batch in enumerate(pf.iter_batches(batch_size=batch_rows)):
            table = pa.Table.from_batches([batch])
            read += table.num_rows
            table = predicate(table)
            kept += table.num_rows
            if i % LOG_EVERY == 0:
                print(f"  {read}/{total} leidas ({100 * read / total:.0f}%), {kept} conservadas, "
                      f"{time.time() - t0:.0f}s, RAM pico {peak_mb():.0f} MB", flush=True)
            if table.num_rows:
                yield table
        print(f"  fin de lectura: {read} leidas, {kept} conservadas, {time.time() - t0:.0f}s, "
              f"RAM pico {peak_mb():.0f} MB", flush=True)


# ---------- escritura: ruta fija, sobrescribe, y atomica ----------
def write_stream(fs, path, tables):
    """Escribe con autocommit=False: el objeto solo aparece en MinIO si TODO sale bien.
    Si falla a mitad, se aborta la subida y no queda un archivo parcial."""
    f = fs.open(path, "wb", autocommit=False)
    writer = None
    total = 0
    try:
        for table in tables:
            if writer is None:
                writer = pq.ParquetWriter(f, table.schema, compression="snappy")
            writer.write_table(table)
            total += table.num_rows
        if writer is None:
            raise RuntimeError("No hubo filas para escribir: revisa el periodo o la fuente")
        writer.close()
        f.close()
        f.commit()
    except Exception:
        try:
            f.discard()
        except Exception:
            pass  # no tapar el error original
        raise
    return total


# ---------- datasets ----------
def seed_posts(period, src=None, dst=None):
    src, dst = src or fs_source(), dst or fs_minio()
    start, end = config.period_window(period)
    src_path = f"{config.SOURCE_BASE}/posts/{start.year}.parquet"
    dst_path = config.bronze_path("posts", period)
    print(f"posts {period}: {src_path} -> {dst_path}")
    tables = stream_filtered(src, src_path, lambda t: window_filter(t, start, end))
    return dst_path, write_stream(dst, dst_path, tables)


def author_ids(dst, period):
    with dst.open(config.bronze_path("posts", period), "rb") as f:
        owners = pq.read_table(f, columns=["OwnerUserId"])["OwnerUserId"]
    ids = pc.unique(owners)
    return ids.filter(pc.greater(ids, 0))  # 0 = sin autor en el esquema de ClickHouse


def seed_users(period, src=None, dst=None):
    src, dst = src or fs_source(), dst or fs_minio()
    ids = author_ids(dst, period)
    src_path = f"{config.SOURCE_BASE}/users.parquet"
    dst_path = config.bronze_path("users", period)
    print(f"users {period}: {len(ids)} autores distintos; {src_path} -> {dst_path}")
    tables = stream_filtered(src, src_path, lambda t: t.filter(pc.is_in(t["Id"], value_set=ids)))
    return dst_path, write_stream(dst, dst_path, tables)


def main(argv):
    if len(argv) != 3 or argv[1] not in ("posts", "users"):
        sys.exit("Uso: python bronze_seed.py <posts|users> <periodo>   (periodos: "
                 + ", ".join(sorted(config.PERIODS)) + ")")
    dataset, period = argv[1], argv[2]
    t0 = time.time()
    path, rows = (seed_posts if dataset == "posts" else seed_users)(period)
    size = fs_minio().size(path) / 1e6
    print(f"LISTO: {rows} filas, {size:.1f} MB en s3://{path} ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main(sys.argv)