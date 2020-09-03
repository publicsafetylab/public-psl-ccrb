import pandas as pd
import boto3
from io import StringIO

S3 = boto3.resource("s3")
BUCKET = "jdi-ccrb"
conn = S3.Bucket(BUCKET)
fns = [object_summary.key for object_summary in conn.objects.filter(Prefix=f"stops-raw")][1:]
dfs = []

for fn in fns:
    print(f"Attempting:  {fn.split('/')[1]}")
    try: dfs.append(pd.read_csv(StringIO(S3.Bucket(BUCKET).Object(fn).get()["Body"].read().decode("utf-8"))))
    except: dfs.append(pd.read_csv(StringIO(S3.Bucket(BUCKET).Object(fn).get()["Body"].read().decode("iso-8859-1"))))

cts_df = pd.DataFrame(list(zip(range(2003,2020), [len(df) for df in dfs])), columns=["Year", "STOPS"])
csv_buffer = StringIO()
cts_df.to_csv(csv_buffer)
S3.Object(BUCKET, "stop-counts.csv").put(Body=csv_buffer.getvalue())

ccrb = pd.read_csv("ccrb_merged.csv")
if 'STOPS' not in ccrb.columns: pd.merge(ccrb, cts_df, how="left", on="Year").to_csv("ccrb_merged.csv", index=False)
else: print(f"STOPS already merged")