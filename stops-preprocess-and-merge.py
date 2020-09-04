import pandas as pd
import numpy as np
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

pct_cts_dfs = []
cts_df = pd.DataFrame(list(zip(range(2003,2020), [len(df) for df in dfs])), columns=["Year", "YR_STOPS"])

for df in dfs:
    try: pct_cts_dfs.append(pd.DataFrame(df.groupby(["pct","year"]).size(), columns=["PCT_YR_STOPS"]).reset_index().rename(columns={"pct":"Precinct","year":"Year"}))
    except: pct_cts_dfs.append(pd.DataFrame(df.groupby(["STOP_LOCATION_PRECINCT","YEAR2"]).size(), columns=["PCT_YR_STOPS"]).reset_index().rename(columns={"STOP_LOCATION_PRECINCT":"Precinct","YEAR2":"Year"}))

pct_cts_df = pd.concat(pct_cts_dfs).reset_index().drop(columns=["index"])
pct_cts_df = pct_cts_df[pct_cts_df["Year"]!= " "]
pct_cts_df["Year"] = pct_cts_df["Year"].astype(int)
pct_cts_df["Precinct"] = np.where(pct_cts_df["Precinct"].isin([" ", "#NULL!","208760","999"]),"999",pct_cts_df["Precinct"])
pct_cts_df["Precinct"] = pct_cts_df["Precinct"].astype(int)
df = pct_cts_df.merge(cts_df, how="left", on="Year")

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
S3.Object(BUCKET, "stop-counts.csv").put(Body=csv_buffer.getvalue())
ccrb = pd.read_csv("ccrb_merged.csv")

if "YR_STOPS" not in ccrb.columns and "PCT_YR_STOPS" not in ccrb.columns: 
    ccrb = pd.merge(ccrb, cts_df, how="left", on="Year")
    ccrb = pd.merge(ccrb, pct_cts_df, how="left", on=["Precinct","Year"])
    ccrb.to_csv("ccrb_merged.csv", index=False)
else: print(f"STOPS already merged")