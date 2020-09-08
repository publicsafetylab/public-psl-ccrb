from io import StringIO

import boto3
import numpy as np
import pandas as pd
import preprocess

##  connect to s3 to ingest csvs
S3 = boto3.resource("s3")
BUCKET = "jdi-ccrb"
conn = S3.Bucket(BUCKET)

##  ingest raw csv and extract year
print("Reading in raw CCRB data")
ccrb_raw = pd.read_csv("s3://jdi-ccrb/complaints-raw/CCRB_database_raw.csv")
ccrb_raw["Year"] = pd.to_datetime(ccrb_raw["Incident Date"]).dt.year.fillna("-1").astype(int)
ccrb = ccrb_raw.copy()

##  ingest census data and merge
print("Reading in census data")
census2010_raw = pd.read_csv("s3://jdi-ccrb/census-raw/nyc_2010censusblocks_2020policeprecincts.csv")
census2010 = preprocess.census(census2010_raw)
census2000 = pd.read_csv('raw/nyc2000/precinct20_demos00.csv')
census = pd.merge(census2010, census2000, how='left', on='precinct_2020')
census["Precinct_Str"] = census["precinct_2020"].apply(lambda p: str(int(p)).zfill(3))
ccrb["Precinct"] = np.where(ccrb["Command"].str.endswith(" PCT"), ccrb["Command"].str.replace(" PCT", ""),
                            ccrb["Command"])
merged = ccrb.join(census.set_index("Precinct_Str"), how="left", on="Precinct")
merged["Precinct"] = np.where(merged["Precinct"].str.isnumeric(), merged["Precinct"], "-1")
merged["Precinct"] = merged["Precinct"].astype(int)

##  ingest incidents data and merge
print("Reading in crime complaints data")
complaints = pd.DataFrame()
chunk_rows = 10000
i = 1
for chunk in pd.read_csv("s3://jdi-ccrb/crime-complaints-raw/NYPD_Complaint_Data_Historic.csv", chunksize=10000):
    print(f"Reading Complaints csv row {(i - 1) * chunk_rows + 1} - {i * chunk_rows}")
    chunk['YEAR'] = pd.to_datetime(chunk['CMPLNT_FR_DT'], errors="coerce").dt.year
    complaints = pd.concat([complaints, chunk[chunk['YEAR'] >= 1980]])
    i += 1
otypes = pd.read_csv(
    StringIO(S3.Bucket(BUCKET).Object("crime-complaints-raw/offensetypes.csv").get()["Body"].read().decode("utf-8")))
cdf = pd.merge(complaints, otypes[['OFNS_DESC', 'OFNS_TYPE']], how='left', on='OFNS_DESC')
table = pd.pivot_table(cdf, index=['YEAR', 'ADDR_PCT_CD'], columns=['OFNS_TYPE'], aggfunc='size', fill_value=0)
table = table.reset_index()
table[['YEAR', 'ADDR_PCT_CD']] = table[['YEAR', 'ADDR_PCT_CD']].astype(int)
merged = pd.merge(merged, table, how='left', left_on=['Year', 'Precinct'], right_on=['YEAR', 'ADDR_PCT_CD']).drop(
    labels=['YEAR', 'ADDR_PCT_CD'], axis=1)

##  ingest number of officers and merge
print("Reading in number of officers data")
off = pd.read_csv(
    StringIO(S3.Bucket(BUCKET).Object("kaplan-raw/police_count.csv").get()["Body"].read().decode("utf-8")))
off = off[["year", "population", "total_employees_officers", "total_employees_total"]].rename(
    columns={"year": "Year", "population": "YR_CITY_POP", "total_employees_officers": "YR_NUM_OFFICERS",
             "total_employees_total": "YR_NUM_NYPD_EMPLOYEES"})
ccrb = pd.merge(merged, off, how="left", on="Year")

##  ingest arrest count and merge
print("Reading in number of arrests data")
arr = pd.read_csv(
    StringIO(S3.Bucket(BUCKET).Object("kaplan-raw/arrests_count.csv").get()["Body"].read().decode("utf-8")))
arr = arr[["year", "all_arrests_total_tot_arrests"]].rename(
    columns={"year": "Year", "all_arrests_total_tot_arrests": "YR_ARRESTS"})
ccrb = pd.merge(ccrb, arr, how="left", on="Year")

##  ingest offense count and merge
print("Reading in number of offenses data")
crm = pd.read_csv(
    StringIO(S3.Bucket(BUCKET).Object("kaplan-raw/offenses_count.csv").get()["Body"].read().decode("utf-8")))
crm = arr[["year", "actual_all_crimes", "tot_clr_all_crimes"]].rename(
    columns={"year": "Year", "actual_all_crimes": "YR_OFFENSES", "tot_clr_all_crimes": "YR_OFFENSES_CLEARED"})
ccrb = pd.merge(ccrb, crm, how="left", on="Year")

##  collect large stops csvs from s3 and merge
print("Reading in stops data")
fns = [object_summary.key for object_summary in conn.objects.filter(Prefix=f"stops-raw")][1:]
dfs = []

for fn in fns:
    print(f"Attempting:  {fn.split('/')[1]}")
    try:
        dfs.append(pd.read_csv(StringIO(S3.Bucket(BUCKET).Object(fn).get()["Body"].read().decode("utf-8"))))
    except:
        dfs.append(pd.read_csv(StringIO(S3.Bucket(BUCKET).Object(fn).get()["Body"].read().decode("iso-8859-1"))))

pct_cts_dfs = []
dfs_stripped = []

for df in dfs:
    try:
        df["pct"] = df["pct"].apply(lambda x: str(x).strip())
    except:
        df["STOP_LOCATION_PRECINCT"] = df["STOP_LOCATION_PRECINCT"].apply(lambda x: str(x).strip())
    dfs_stripped.append(df)

pct_cts_dfs = []
cts_df = pd.DataFrame(list(zip(range(2003, 2020), [len(df) for df in dfs_stripped])), columns=["Year", "YR_STOPS"])

for df in dfs_stripped:
    try:
        df["year"] = df["year"].apply(lambda x: str(x).strip())
        df = df[df["year"] != ""]
        pct_cts_dfs.append(
            pd.DataFrame(df.groupby(["pct", "year"]).size(), columns=["PCT_YR_STOPS"]).reset_index().rename(
                columns={"pct": "Precinct", "year": "Year"}))
    except:
        df["YEAR2"] = df["YEAR2"].astype(int)
        if df["YEAR2"][0] == 2017.0: df["YEAR2"] = 2017
        df = df[df["YEAR2"] != ""]
        pct_cts_dfs.append(pd.DataFrame(df.groupby(["STOP_LOCATION_PRECINCT", "YEAR2"]).size(),
                                        columns=["PCT_YR_STOPS"]).reset_index().rename(
            columns={"STOP_LOCATION_PRECINCT": "Precinct", "YEAR2": "Year"}))

pct_cts_df = pd.concat(pct_cts_dfs).reset_index().drop(columns=["index"])
pct_cts_df["Year"] = pct_cts_df["Year"].astype(int)
pct_cts_df["Precinct"] = np.where(pct_cts_df["Precinct"].isin([" ", "#NULL!", "208760", "999", ""]), "999",
                                  pct_cts_df["Precinct"])
pct_cts_df["Precinct"] = pct_cts_df["Precinct"].astype(int)
ccrb = pd.merge(ccrb, cts_df, how="left", on="Year")
ccrb = pd.merge(ccrb, pct_cts_df, how="left", on=["Precinct", "Year"])

##  check value counts from raw and processed
print("Checking value counts")
vals_df = pd.DataFrame(ccrb.groupby("Year").size()).reset_index().rename(columns={"index": "Year", 0: "Count"})
raw_vals_df = pd.DataFrame(ccrb_raw["Year"].value_counts()).reset_index().rename(
    columns={"index": "Year", "Year": "Raw_Count"})
both_vals_df = pd.merge(vals_df, raw_vals_df, on="Year")
if len(both_vals_df[both_vals_df["Count"] != both_vals_df["Raw_Count"]]) < 1:
    print("Value counts match")
    ccrb.to_csv("out/ccrb_merged.csv", index=False)
    csv_buffer = StringIO()
    ccrb.to_csv(csv_buffer, index=False)
    S3.Object(BUCKET, "ccrb_merged.csv").put(Body=csv_buffer.getvalue())
else:
    raise Exception("Mismatch between raw and process counts")
