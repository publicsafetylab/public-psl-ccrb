from itertools import product
from io import StringIO
import pandas as pd
import numpy as np
import warnings
import boto3
import json
import os

# Note: suppressing warnings optional
warnings.simplefilter(action="ignore", category=Warning)
print("\n" + "*"*20 + "\n")

# Connect to public AWS S3 bucket
# Hosted by NYU's Public Safety Lab @ https://psl-ccrb.s3.amazonaws.com/
S3 = boto3.resource("s3")
BUCKET = "psl-ccrb"
print(f"Connecting to NYU Public Safety Lab AWS S3 bucket {BUCKET}")
conn = S3.Bucket(BUCKET)

# Ingest raw NYCLU's NYC CCRB data CSV and extract Year and Month from Incident Date (fill missing -1)
# Data provided by NYCLU @ https://github.com/new-york-civil-liberties-union/NYPD-Misconduct-Complaint-Database
print("Reading CCRB raw data")
ccrb = pd.read_csv("s3://psl-ccrb/raw/nyclu-misconduct-complaints.csv")
ccrb["Year"] = pd.to_datetime(ccrb["Incident Date"]).dt.year.fillna("-1").astype(int)
ccrb["Month"] = pd.to_datetime(ccrb["Incident Date"]).dt.month.fillna("-1").astype(int)

# Extract precinct from CCRB Command field (fill missing -1)
precinct_map = json.loads(S3.Bucket(BUCKET).Object("raw/nyclu-misconduct-complaints-precinct-mapping.json").get()["Body"].read().decode("utf-8"))
precinct_map = dict(zip([d["Command"].strip() for d in precinct_map], [d["Complaints_Pct"].strip() for d in precinct_map]))
ccrb["Precinct"] = ccrb["Command"].replace(precinct_map)
ccrb["Precinct"] = np.where(ccrb["Precinct"].isin(precinct_map.values()), ccrb["Precinct"], "-1")

# Ingest 2010 US Census data mapped to 2020 NYPD precincts
# Data provided by John Keefe @ https://johnkeefe.net/nyc-police-precinct-and-census-data)
print("Reading Keefe 2010 Census 2020 NYPD precinct mapped data")
census = pd.read_csv("s3://psl-ccrb/raw/keefe-census-2010-precinct-2020-mapping.csv")
census_map = json.loads(S3.Bucket(BUCKET).Object("raw/keefe-census-2010-column-mapping.json").get()["Body"].read().decode("utf-8"))
census = census.rename(columns=census_map)
census = census.drop([r for r in census.columns if r.startswith("P00")], axis=1)
census_records = census.to_dict(orient="records")

# Extract population counts by demographic group
black_columns = [c for c in census.columns if c.startswith("R_") and "B" in c]
nh_black_columns = [c for c in census.columns if c.startswith("NH_") and "B" in c]
nh_asian_columns = [c for c in census.columns if c.startswith("NH_") and "A" in c and "B" not in c]
for r in census_records:
    r["Black"] = sum([r[c] for c in black_columns])
    r["NH_Black"] = sum([r[c] for c in nh_black_columns])  # intermediate
    r["H_Black"] = r["Black"] - r["NH_Black"]  # intermediate
    r["Hispanics"] = r["Hispanics"] - r["H_Black"]
    r["NH_Asian"] = sum([r[c] for c in nh_asian_columns])
    r["NH_White"] = r["NH_W"]
    r["Others"] = r["Total_Population"] - r["Black"] - r["Hispanics"] - r["NH_Asian"] - r["NH_White"]
    
# Calculate demographic proportions and merge per-precinct into CCRB data
census = pd.DataFrame.from_dict(census_records)
census = census.dropna(subset=["precinct_2020"])
precinct_groups = census.groupby("precinct_2020")
demo = precinct_groups[["Total_Population", "Black", "Hispanics", "NH_Asian", "NH_White", "Others"]].sum().reset_index()
demo["Black_Percent"] = demo["Black"]/demo["Total_Population"]
demo["Hispanic_Percent"] = demo["Hispanics"]/demo["Total_Population"]
demo["NH_Asian_Percent"] = demo["NH_Asian"]/demo["Total_Population"]
demo["NH_White_Percent"] = demo["NH_White"]/demo["Total_Population"]
demo["Other_Percent"] = demo["Others"]/demo["Total_Population"]
census = demo.rename(columns={"Total_Population": "Total_Pop", "Others": "Other_Pop", "Hispanics": "Hispanic_Pop", "Black": "Black_Pop", "NH_Asian": "NH_Asian_Pop", "NH_White": "NH_White_Pop"})
census["Census_Precinct"] = census["precinct_2020"].apply(lambda p: str(int(p)))
ccrb = ccrb.join(census.set_index("Census_Precinct"), how="left", on="Precinct").drop(columns=["precinct_2020"])

# Ingest number of NYPD officers per year and merge
# Data provided by Jacob Kaplan @ https://jacobdkaplan.com/
print("Reading Kaplan NYPD officers data")
num_officers = pd.read_csv("s3://psl-ccrb/raw/kaplan-police.csv")
num_officers = num_officers[["year", "population", "total_employees_officers", "total_employees_total"]].rename(columns={"year": "Year", "population": "NYC_Pop_Year", "total_employees_officers": "Num_NYPD_Officers_Year", "total_employees_total": "Num_NYPD_Employees_Year"})
ccrb = pd.merge(ccrb, num_officers, how="left", on="Year")

# Ingest number of arrests per year and merge
# Data provided by Jacob Kaplan @ https://jacobdkaplan.com/
print("Reading Kaplan NYC arrests data")
num_arrests = pd.read_csv("s3://psl-ccrb/raw/kaplan-arrests.csv")
num_arrests = num_arrests[["year", "all_arrests_total_tot_arrests"]].rename(columns={"year": "Year", "all_arrests_total_tot_arrests": "Num_Arrests_Year"})
ccrb = pd.merge(ccrb, num_arrests, how="left", on="Year")

# Ingest number of offenses per year and merge
# Data provided by Jacob Kaplan @ https://jacobdkaplan.com/
print("Reading Kaplan NYC offenses data")
num_offenses = pd.read_csv("s3://psl-ccrb/raw/kaplan-offenses.csv")
num_offenses = num_offenses[["year", "actual_all_crimes", "tot_clr_all_crimes"]].rename(columns={"year": "Year", "actual_all_crimes": "Num_Offenses_Year", "tot_clr_all_crimes": "Num_Offenses_Cleared_Year"})
ccrb = pd.merge(ccrb, num_offenses, how="left", on="Year")

# Ingest NYPD stop-and-frisk data
# Data provided by NYC/NYPD @ https://www1.nyc.gov/site/nypd/stats/reports-analysis/stopfrisk.page
# Note: DtypeWarning can be suppressed by specifying column data types
fns = [object_summary.key for object_summary in conn.objects.filter(Prefix=f"raw/nyclu-stops-")]
dfs = []
for fn in fns:
    print(f"Reading NYPD stop-and-frisk yearly file for {fn.split('-')[-1].split('.')[0]}")
    try:
        dfs.append(pd.read_csv(StringIO(S3.Bucket(BUCKET).Object(fn).get()["Body"].read().decode("utf-8"))))
    except:
        dfs.append(pd.read_csv(StringIO(S3.Bucket(BUCKET).Object(fn).get()["Body"].read().decode("iso-8859-1"))))
        
# Function to extract month from some stops CSVs
def extract_stops_month(s):
    s = str(s).strip()
    if s=="":
        return -1
    elif len(s)==7:
        return int(str(s)[0])
    elif len(s)==8:
        return int(str(s)[0:2])
    elif "-" in s:
        return int(s.split("-")[1])
    else:
        raise ValueError

# Function to extract precinct from stops CSVs
def extract_stops_precinct(s):
    s = str(s).strip()
    if not s.isdigit():
        return "-1"
    elif s==999:
        return "-1"
    else:
        return s
    
# Process NYPD stop-and-frisk data to collect counts by year, month, precinct-year and precinct-month
mo_precinct_stops_counts_dfs = []
for df in dfs:
    try:
        df = df.rename(columns={"year": "Year", "pct": "Precinct"})
        df["Month"] = df["datestop"].apply(lambda d: extract_stops_month(d))
    except:
        df = df.rename(columns={"YEAR2": "Year", "STOP_LOCATION_PRECINCT": "Precinct"})
        df["Month"] = pd.to_datetime(df["STOP_FRISK_DATE"]).dt.month.fillna("-1").astype(int)
    df["Year"] = int(df["Year"][0])
    df["Precinct"] = df["Precinct"].apply(lambda p: extract_stops_precinct(p))
    mo_precinct_stops_counts_dfs.append(pd.DataFrame(df.groupby(["Year", "Month", "Precinct"]).size(), columns=["Stops_Precinct_Month"]).reset_index())
mo_precinct_stops_counts_df = pd.concat(mo_precinct_stops_counts_dfs).reset_index().drop(columns=["index"])
yr_precinct_stops_counts_df = mo_precinct_stops_counts_df.groupby(["Year", "Precinct"])["Stops_Precinct_Month"].sum().reset_index().rename(columns={"Stops_Precinct_Month": "Stops_Precinct_Year"})
mo_stops_counts_df = mo_precinct_stops_counts_df.groupby(["Year", "Month"])["Stops_Precinct_Month"].sum().reset_index().rename(columns={"Stops_Precinct_Month": "Stops_Month"})
yr_stops_counts_df = mo_stops_counts_df.groupby(["Year"])["Stops_Month"].sum().reset_index().rename(columns={"Stops_Month": "Stops_Year"})

# Merge stops counts by year, month, precinct-year and precinct-month
ccrb = pd.merge(ccrb, yr_stops_counts_df, how="left", on="Year")
ccrb = pd.merge(ccrb, yr_precinct_stops_counts_df, how="left", on=["Precinct", "Year"])
ccrb = pd.merge(ccrb, mo_stops_counts_df, how="left", on=["Year", "Month"])
ccrb = pd.merge(ccrb, mo_precinct_stops_counts_df, how="left", on=["Year", "Month", "Precinct"])

# Save intermediate CSV to tmp directory of S3 bucket
print("Saving intermediate data to s3://psl-ccrb/tmp/")
ccrb.to_csv("s3://psl-ccrb/tmp/ccrb-minus-crime-complaints.csv", index=False)

# Ingest NYPD crime complaints file
# Data provided by NYC Open Data @ https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i
# Note: chunk_rows optional, specifies chunksize
# Note: DtypeWarning can be suppressed by specifying column data types
complaints = pd.DataFrame()
chunk_rows = 2000000
i = 1
for chunk in pd.read_csv("s3://psl-ccrb/raw/nypd-crime-complaints.csv", chunksize=chunk_rows):
    print(f"Reading NYC Open Data crime complaint data rows {(i-1)*chunk_rows+1}-{i*chunk_rows}")
    complaints = pd.concat([complaints, chunk])
    i += 1
    
# Read in offense type mapping JSON and mere with crime complaints
offense_types = pd.read_csv("s3://psl-ccrb/raw/nypd-crime-complaints-type-mapping.csv")
complaints_df = pd.merge(complaints, offense_types[["OFNS_DESC", "OFNS_TYPE"]], how="left", on="OFNS_DESC")
complaints_df = complaints_df[complaints_df["OFNS_TYPE"].notnull()]

# Select years from 1980 to present
complaints_df["YEAR"] = pd.to_datetime(complaints_df["CMPLNT_FR_DT"], errors="coerce").dt.year.fillna(-1).astype(int)
complaints_df["MONTH"] = pd.to_datetime(complaints_df["CMPLNT_FR_DT"], errors="coerce").dt.month.fillna(-1).astype(int)
complaints_df = complaints_df[complaints_df["YEAR"]>=1980]

# Where transit district provided, overwrite precinct
complaints_df["ADDR_PCT_CD"] = np.where(complaints_df["ADDR_PCT_CD"].isnull(), complaints_df["TRANSIT_DISTRICT"], complaints_df["ADDR_PCT_CD"])
complaints_df["ADDR_PCT_CD"] = complaints_df["ADDR_PCT_CD"].fillna(-1.0).astype(int)
complaints_df["TRANSIT_DISTRICT"] = complaints_df["TRANSIT_DISTRICT"].fillna(-1.0).astype(int)
complaints_df[["ADDR_PCT_CD", "TRANSIT_DISTRICT"]] = complaints_df[["ADDR_PCT_CD", "TRANSIT_DISTRICT"]].astype(str)
complaints_df["TRANSIT_DISTRICT"] = np.where(complaints_df["TRANSIT_DISTRICT"]!="-1", "TD" + complaints_df["TRANSIT_DISTRICT"], complaints_df["TRANSIT_DISTRICT"])
complaints_df["ADDR_PCT_CD"] = np.where(complaints_df["TRANSIT_DISTRICT"]!="-1", complaints_df["TRANSIT_DISTRICT"], complaints_df["ADDR_PCT_CD"])

# Collect crime complaint counts by year and month
crime_complaints_yearly = complaints_df.groupby("YEAR")["OFNS_TYPE"].value_counts().unstack().fillna(0.0).reset_index()
crime_complaints_monthly = complaints_df.groupby(["YEAR", "MONTH"])["OFNS_TYPE"].value_counts().unstack().fillna(0.0).reset_index()
for c in crime_complaints_yearly.columns:
    if c not in ["YEAR"]:
        crime_complaints_yearly = crime_complaints_yearly.rename(columns={c:f"Num_Crime_Complaints_{c.capitalize()}_Year"})
for c in crime_complaints_monthly.columns:
    if c not in ["YEAR", "MONTH"]:
        crime_complaints_monthly = crime_complaints_monthly.rename(columns={c:f"Num_Crime_Complaints_{c.capitalize()}_Month"})
crime_complaints = pd.merge(crime_complaints_monthly, crime_complaints_yearly, on=["YEAR"])

# Save intermediate CSV to tmp directory of S3 bucket
crime_complaints.to_csv("s3://psl-ccrb/tmp/nypd-crime-complaints-count-by-year-month.csv", index=False)

# Collect crime complaint counts by precinct-year and precinct-month
precinct_crime_complaints_yearly = complaints_df.groupby(["YEAR", "ADDR_PCT_CD"])["OFNS_TYPE"].value_counts().unstack().fillna(0.0).reset_index()
precinct_crime_complaints_monthly = complaints_df.groupby(["YEAR", "MONTH", "ADDR_PCT_CD"])["OFNS_TYPE"].value_counts().unstack().fillna(0.0).reset_index()
for c in precinct_crime_complaints_yearly.columns:
    if c not in ["YEAR", "ADDR_PCT_CD"]:
        precinct_crime_complaints_yearly = precinct_crime_complaints_yearly.rename(columns={c:f"Num_Crime_Complaints_{c.capitalize()}_Precinct_Year"})
for c in precinct_crime_complaints_monthly.columns:
    if c not in ["YEAR", "MONTH", "ADDR_PCT_CD"]:
        precinct_crime_complaints_monthly = precinct_crime_complaints_monthly.rename(columns={c:f"Num_Crime_Complaints_{c.capitalize()}_Precinct_Month"})
precinct_crime_complaints = pd.merge(precinct_crime_complaints_monthly, precinct_crime_complaints_yearly, on=["YEAR", "ADDR_PCT_CD"])

# Save intermediate CSV to tmp directory of S3 bucket
precinct_crime_complaints.to_csv("s3://psl-ccrb/tmp/nypd-crime-complaints-count-by-precinct-year-month.csv", index=False)

# Merge crime complaints and finalize
final = pd.merge(ccrb, crime_complaints, how="left", left_on=["Year", "Month"], right_on=["YEAR", "MONTH"])
final = pd.merge(final, precinct_crime_complaints, how="left", left_on=["Year", "Month", "Precinct"], right_on=["YEAR", "MONTH", "ADDR_PCT_CD"])
final = final.drop(columns={"YEAR_x", "MONTH_x", "YEAR_y", "MONTH_y", "ADDR_PCT_CD"})

# Save final CSV to out directory of S3 bucket
print("Saving final data to s3://psl-ccrb/out/")
final.to_csv("s3://psl-ccrb/out/data.csv", index=False)

# Save final CSV to chunks under GitHub size limit in out directory
cols = list(final.columns)
final.to_csv("data.csv", index=False)
if not os.path.isdir("out"):
    os.system("mkdir out")
os.system("split -a 1 -l 60000 data.csv out/data_chunk_")
os.system("for file in out/data_chunk_*; do mv ${file} ${file}.csv; done")
os.remove("data.csv")
for i, f in enumerate(sorted(os.listdir("out"))):
    if i==0:
        tmp = pd.read_csv(f"out/{f}")
    else:
        tmp = pd.read_csv(f"out/{f}", names=cols)
    os.remove(f"out/{f}")
    tmp.to_csv(f"out/{'_'.join(f.split('_')[0:2])}_{i}.csv", index=False)
print("\n" + "*"*20 + "\n")