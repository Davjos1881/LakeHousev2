import s3fs
import pyarrow.parquet as pq

fs = s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": "eu-west-3"})
BASE = "datasets-documentation/stackoverflow/parquet/"

for key in ["posts/2020.parquet", "posts/2021.parquet", "users.parquet"]:
    path = BASE + key
    print("==", key, round(fs.size(path) / 1e6), "MB")
    f = pq.ParquetFile(fs.open(path))
    print(f.metadata.num_rows, "filas,", f.metadata.num_row_groups, "row groups")
    print(f.schema_arrow)