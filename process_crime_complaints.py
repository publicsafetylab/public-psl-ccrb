from io import StringIO
import boto3
import numpy as np
import pandas as pd
import json

##  connect to s3 to ingest csvs
S3 = boto3.resource("s3")
BUCKET = "jdi-ccrb"
conn = S3.Bucket(BUCKET)

##  read in partially processed ccrb data
ccrb = pd.read_csv('s3://jdi-ccrb/tmp/ccrb-minus-crime-complaints.csv')

##  read in large nypd crime complaints file, chunk_rows specifies chunk size
complaints = pd.DataFrame()
chunk_rows = 2000000
i = 1
for chunk in pd.read_csv("s3://jdi-ccrb/raw/nypd-crime-complaints.csv", chunksize=chunk_rows):
    print(f"Reading Complaints csv row {(i - 1) * chunk_rows + 1} - {i * chunk_rows}")
    complaints = pd.concat([complaints, chunk])
    i += 1
    print(complaints.shape)

##  read in offense types, merge and process data types
otypes = pd.read_csv("s3://jdi-ccrb/raw/nypd-crime-complaints-type-mapping.csv")
cdf = pd.merge(complaints, otypes[['OFNS_DESC', 'OFNS_TYPE']], how='left', on='OFNS_DESC')
cdf = cdf[cdf['OFNS_TYPE'].notnull()]
cdf['YEAR'] = pd.to_datetime(cdf['CMPLNT_FR_DT'], errors="coerce").dt.year.fillna(-1).astype(int)
cdf['MONTH'] = pd.to_datetime(cdf['CMPLNT_FR_DT'], errors="coerce").dt.month.fillna(-1).astype(int)
cdf = cdf[cdf['YEAR']>=1980]
cdf['ADDR_PCT_CD'] = np.where(cdf['ADDR_PCT_CD'].isnull(), cdf['TRANSIT_DISTRICT'], cdf['ADDR_PCT_CD'])
cdf['ADDR_PCT_CD'] = cdf['ADDR_PCT_CD'].fillna(-1.0).astype(int)
cdf['TRANSIT_DISTRICT'] = cdf['TRANSIT_DISTRICT'].fillna(-1.0).astype(int)
cdf[['ADDR_PCT_CD','TRANSIT_DISTRICT']] = cdf[['ADDR_PCT_CD','TRANSIT_DISTRICT']].astype(str)
cdf['TRANSIT_DISTRICT'] = np.where(cdf['TRANSIT_DISTRICT']!='-1', 'TD' + cdf['TRANSIT_DISTRICT'], cdf['TRANSIT_DISTRICT'])
cdf['ADDR_PCT_CD'] = np.where(cdf['TRANSIT_DISTRICT']!='-1', cdf['TRANSIT_DISTRICT'], cdf['ADDR_PCT_CD'])

##  capture crime complaint counts per year and month, export to csv in s3 tmp
crime_complaints_yearly = cdf.groupby('YEAR')['OFNS_TYPE'].value_counts().unstack().fillna(0.0).reset_index()
crime_complaints_monthly = cdf.groupby(['YEAR','MONTH'])['OFNS_TYPE'].value_counts().unstack().fillna(0.0).reset_index()
for c in crime_complaints_yearly.columns:
    if c not in ['YEAR']:
        crime_complaints_yearly = crime_complaints_yearly.rename(columns={c:f"YR_{c}"})
for c in crime_complaints_monthly.columns:
    if c not in ['YEAR','MONTH']:
        crime_complaints_monthly = crime_complaints_monthly.rename(columns={c:f"MONTH_{c}"})
crime_complaints = pd.merge(crime_complaints_monthly, crime_complaints_yearly, on=['YEAR'])
crime_complaints.to_csv('s3://jdi-ccrb/tmp/nypd-crime-complaints-count-by-year-month.csv', index=False)

##  capture crime complaint counts per precinct-year and precinct-month, export to csv in s3 tmp
crime_complaints_yearly = cdf.groupby(['YEAR','ADDR_PCT_CD'])['OFNS_TYPE'].value_counts().unstack().fillna(0.0).reset_index()
crime_complaints_monthly = cdf.groupby(['YEAR','MONTH','ADDR_PCT_CD'])['OFNS_TYPE'].value_counts().unstack().fillna(0.0).reset_index()
for c in crime_complaints_yearly.columns:
    if c not in ['YEAR','ADDR_PCT_CD']:
        crime_complaints_yearly = crime_complaints_yearly.rename(columns={c:f"YR_{c}"})
for c in crime_complaints_monthly.columns:
    if c not in ['YEAR','MONTH','ADDR_PCT_CD']:
        crime_complaints_monthly = crime_complaints_monthly.rename(columns={c:f"MONTH_{c}"})
crime_complaints = pd.merge(crime_complaints_monthly, crime_complaints_yearly, on=['YEAR','ADDR_PCT_CD'])
crime_complaints.to_csv('s3://jdi-ccrb/tmp/nypd-crime-complaints-count-by-precinct-year-month.csv', index=False)

##  reread in csvs and perform final process
c_no_pct = pd.read_csv('s3://jdi-ccrb/tmp/nypd-crime-complaints-count-by-year-month.csv')
c_pct = pd.read_csv('s3://jdi-ccrb/tmp/nypd-crime-complaints-count-by-precinct-year-month.csv')
out = pd.merge(ccrb, c_no_pct, how='left', left_on=['Year','Month'], right_on=['YEAR','MONTH'])
for c in c_pct.columns:
    if c not in ['YEAR','MONTH','ADDR_PCT_CD']:
        c_pct = c_pct.rename(columns={c:f"PCT_{c}"})
final = pd.merge(out, c_pct, how='left', left_on=['Year','Month','Precinct'], right_on=['YEAR','MONTH','ADDR_PCT_CD'])
final.to_csv('s3://jdi-ccrb/out/data.csv', index=False)
final.to_csv('data.csv', index=False)