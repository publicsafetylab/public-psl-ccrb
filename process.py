from io import StringIO
import pandas as pd
import numpy as np
import warnings
import boto3
import json

# Note: suppressing warnings optional
warnings.simplefilter(action="ignore", category=Warning)
print("\n" + "*"*20 + "\n")

# Connect to public AWS S3 bucket
# Hosted by NYU's Public Safety Lab @ https://jdi-ccrb.s3.amazonaws.com/
S3 = boto3.resource("s3")
BUCKET = "jdi-ccrb"
print(f"Connecting to NYU Public Safety Lab AWS S3 bucket {BUCKET}")
conn = S3.Bucket(BUCKET)

# Ingest raw NYCLU's NYC CCRB data CSV and extract Year and Month from Incident Date (fill missing -1)
# Data provided by NYCLU @ https://github.com/new-york-civil-liberties-union/NYPD-Misconduct-Complaint-Database
print("Reading CCRB raw data")
ccrb = pd.read_csv("s3://jdi-ccrb/raw/nyclu-misconduct-complaints.csv")
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
census = pd.read_csv("s3://jdi-ccrb/raw/keefe-census-2010-precinct-2020-mapping.csv")
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
num_officers = pd.read_csv("s3://jdi-ccrb/raw/kaplan-police.csv")
num_officers = num_officers[["year", "population", "total_employees_officers", "total_employees_total"]].rename(columns={"year": "Year", "population": "NYC_Pop_Year", "total_employees_officers": "Num_NYPD_Officers_Year", "total_employees_total": "Num_NYPD_Employees_Year"})
ccrb = pd.merge(ccrb, num_officers, how="left", on="Year")

# Ingest number of arrests per year and merge
# Data provided by Jacob Kaplan @ https://jacobdkaplan.com/
print("Reading Kaplan NYC arrests data")
num_arrests = pd.read_csv("s3://jdi-ccrb/raw/kaplan-arrests.csv")
num_arrests = num_arrests[["year", "all_arrests_total_tot_arrests"]].rename(columns={"year": "Year", "all_arrests_total_tot_arrests": "Num_Arrests_Year"})
ccrb = pd.merge(ccrb, num_arrests, how="left", on="Year")

# Ingest number of offenses per year and merge
# Data provided by Jacob Kaplan @ https://jacobdkaplan.com/
print("Reading Kaplan NYC offenses data")
num_offenses = pd.read_csv("s3://jdi-ccrb/raw/kaplan-offenses.csv")
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
    if type(s)==int:
        if len(str(s))==7: 
            return int(str(s)[0])
        elif len(str(s))==8: 
            return int(str(s)[0:2])
    elif type(s)==str:
        if "-" in s: 
            return int(s.split("-")[1])
        
# Process year, month and precinct fields (fill missing -1), and store counts by year-DataFrame
dfs_processed = []
mo_stops_counts_dfs = []
mo_precinct_stops_counts_dfs = []
yr_precinct_stops_counts_dfs = []
for df in dfs:
    try: 
        df["pct"] = df["pct"].apply(lambda x: str(x).strip())
        df["Month"] = df["datestop"].apply(lambda x: extract_stops_month(x))
        df["year"] = df["year"].apply(lambda x: str(x).strip())
        df = df[df["year"] != ""]
        mo_stops_counts_dfs.append(pd.DataFrame(df.groupby(["year", "Month"]).size(), columns=["Stops_Month"]).reset_index().rename(columns={"year": "Year"}))
        mo_precinct_stops_counts_dfs.append(pd.DataFrame(df.groupby(["pct", "year", "Month"]).size(), columns=["Stops_Precinct_Month"]).reset_index().rename(columns={"pct": "Precinct", "year": "Year"}))
        yr_precinct_stops_counts_dfs.append(pd.DataFrame(df.groupby(["pct", "year"]).size(), columns=["Stops_Precinct_Year"]).reset_index().rename(columns={"pct": "Precinct", "year": "Year"}))
    except:
        df["STOP_LOCATION_PRECINCT"] = df["STOP_LOCATION_PRECINCT"].apply(lambda x: str(x).strip())
        df["Month"] = pd.to_datetime(df["STOP_FRISK_DATE"]).dt.month.fillna("-1").astype(int)
        df["YEAR2"] = df["YEAR2"].astype(int)
        if df["YEAR2"][0] == 2017.0:
            df["YEAR2"] = 2017
        df = df[df["YEAR2"] != ""]
        mo_stops_counts_dfs.append(pd.DataFrame(df.groupby(["YEAR2", "Month"]).size(), columns=["Stops_Month"]).reset_index().rename(columns={"YEAR2": "Year"}))
        mo_precinct_stops_counts_dfs.append(pd.DataFrame(df.groupby(["STOP_LOCATION_PRECINCT", "YEAR2", "Month"]).size(), columns=["Stops_Precinct_Month"]).reset_index().rename(columns={"STOP_LOCATION_PRECINCT": "Precinct", "YEAR2": "Year"}))
        yr_precinct_stops_counts_dfs.append(pd.DataFrame(df.groupby(["STOP_LOCATION_PRECINCT", "YEAR2"]).size(), columns=["Stops_Precinct_Year"]).reset_index().rename(columns={"STOP_LOCATION_PRECINCT": "Precinct", "YEAR2": "Year"}))
    dfs_processed.append(df)

# Concatenate yearly and monthly stops counts
yr_stops_counts_df = pd.DataFrame(list(zip(range(2003, 2020), [len(df) for df in dfs_processed])), columns=["Year", "Stops_Year"])
yr_precinct_stops_counts_df = pd.concat(yr_precinct_stops_counts_dfs).reset_index().drop(columns=["index"])
yr_precinct_stops_counts_df["Year"] = yr_precinct_stops_counts_df["Year"].astype(int)
yr_precinct_stops_counts_df["Precinct"] = np.where(yr_precinct_stops_counts_df["Precinct"].isin([" ", "#NULL!", "208760", "999", ""]), "999", yr_precinct_stops_counts_df["Precinct"])
mo_stops_counts_df = pd.concat(mo_stops_counts_dfs).reset_index().drop(columns=["index"])
mo_stops_counts_df[["Year", "Month"]]= mo_stops_counts_df[["Year", "Month"]].astype(int)
mo_precinct_stops_counts_df = pd.concat(mo_precinct_stops_counts_dfs).reset_index().drop(columns=["index"])
mo_precinct_stops_counts_df[["Year", "Month"]] = mo_precinct_stops_counts_df[["Year", "Month"]].astype(int)
mo_precinct_stops_counts_df["Precinct"] = np.where(mo_precinct_stops_counts_df["Precinct"].isin([" ", "#NULL!", "208760", "999", ""]), "-1", mo_precinct_stops_counts_df["Precinct"])

# Merge stops counts by year, month, precinct-year and precinct-month
ccrb = pd.merge(ccrb, yr_stops_counts_df, how="left", on="Year")
ccrb = pd.merge(ccrb, yr_precinct_stops_counts_df, how="left", on=["Precinct", "Year"])
ccrb = pd.merge(ccrb, mo_stops_counts_df, how="left", on=["Year", "Month"])
ccrb = pd.merge(ccrb, mo_precinct_stops_counts_df, how="left", on=["Precinct", "Year", "Month"])

# Save intermediate CSV to tmp directory of S3 bucket
print("Saving intermediate data to s3://jdi-ccrb/tmp/")
ccrb.to_csv("s3://jdi-ccrb/tmp/ccrb-minus-crime-complaints.csv", index=False)

# Ingest NYPD crime complaints file
# Data provided by NYC Open Data @ https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i
# Note: chunk_rows optional, specifies chunksize
# Note: DtypeWarning can be suppressed by specifying column data types
complaints = pd.DataFrame()
chunk_rows = 2000000
i = 1
for chunk in pd.read_csv("s3://jdi-ccrb/raw/nypd-crime-complaints.csv", chunksize=chunk_rows):
    print(f"Reading NYC Open Data crime complaint data rows {(i-1)*chunk_rows+1}-{i*chunk_rows}")
    complaints = pd.concat([complaints, chunk])
    i += 1
    
# Read in offense type mapping JSON and mere with crime complaints
offense_types = pd.read_csv("s3://jdi-ccrb/raw/nypd-crime-complaints-type-mapping.csv")
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
crime_complaints.to_csv("s3://jdi-ccrb/tmp/nypd-crime-complaints-count-by-year-month.csv", index=False)

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
precinct_crime_complaints.to_csv("s3://jdi-ccrb/tmp/nypd-crime-complaints-count-by-precinct-year-month.csv", index=False)

# Merge crime complaints and finalize
final = pd.merge(ccrb, crime_complaints, how="left", left_on=["Year", "Month"], right_on=["YEAR", "MONTH"])
final = pd.merge(final, precinct_crime_complaints, how="left", left_on=["Year", "Month", "Precinct"], right_on=["YEAR", "MONTH", "ADDR_PCT_CD"])
final = final.drop(columns={"YEAR_x", "MONTH_x", "YEAR_y", "MONTH_y", "ADDR_PCT_CD"})

# Save final CSV to out directory of S3 bucket and to local directory of JDI-CCRB GitHub repository
final.to_csv("s3://jdi-ccrb/out/data.csv", index=False)
final.to_csv("out/data.csv", index=False)
print("\n" + "*"*20 + "\n")