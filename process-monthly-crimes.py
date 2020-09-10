import pandas as pd
import numpy as np
from io import StringIO
import boto3

pd.set_option('display.max_columns',100)
pd.set_option('display.max_rows',1000)
S3 = boto3.resource("s3")
BUCKET = "jdi-ccrb"
conn = S3.Bucket(BUCKET)

ccrb = pd.read_csv(StringIO(S3.Bucket(BUCKET).Object("ccrb_monthly.csv").get()["Body"].read().decode("utf-8")))
otypes = pd.read_csv(StringIO(S3.Bucket(BUCKET).Object("crime-complaints-raw/offensetypes.csv").get()["Body"].read().decode("utf-8")))

complaints = pd.DataFrame()
chunk_rows = 10000
i = 1
for chunk in pd.read_csv("s3://jdi-ccrb/crime-complaints-raw/NYPD_Complaint_Data_Historic.csv", chunksize=10000):
    print(f"Reading Complaints csv row {(i - 1) * chunk_rows + 1} - {i * chunk_rows}")
    chunk['YEAR'] = pd.to_datetime(chunk['CMPLNT_FR_DT'], errors="coerce").dt.year
    chunk['MONTH'] = pd.to_datetime(chunk['CMPLNT_FR_DT'], errors="coerce").dt.month
    complaints = pd.concat([complaints, chunk[chunk['YEAR'] >= 1980]])
    i += 1
    print(complaints.shape)

cdf = pd.merge(complaints, otypes[['OFNS_DESC', 'OFNS_TYPE']], how='left', on='OFNS_DESC')
table = pd.pivot_table(cdf, index=['YEAR', 'MONTH', 'ADDR_PCT_CD'], columns=['OFNS_TYPE'], aggfunc='size', fill_value=0)
table = table.reset_index()
table[['YEAR', 'MONTH', 'ADDR_PCT_CD']] = table[['YEAR', 'MONTH', 'ADDR_PCT_CD']].astype(int)

tcols = {}
for t in table.columns[3:]:
    tcols[t] = 'MONTH_'+t
tcols['YEAR'] = 'YEAR'
tcols['MONTH'] = 'MONTH'
tcols['ADDR_PCT_CD'] = 'ADDR_PCT_CD'
table = table.rename(columns=tcols)

merged = pd.merge(ccrb, table, how='left', left_on=['Year', 'Month', 'Precinct'], right_on=['YEAR', 'MONTH', 'ADDR_PCT_CD']).drop(labels=['YEAR', 'MONTH', 'ADDR_PCT_CD'], axis=1)
merged.to_csv("ccrb_monthly.csv", index=False)