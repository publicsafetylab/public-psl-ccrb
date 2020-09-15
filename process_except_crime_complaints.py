from io import StringIO
import boto3
import numpy as np
import pandas as pd
import preprocess_census
import json

##  connect to s3 to ingest csvs
S3 = boto3.resource("s3")
BUCKET = "jdi-ccrb"
conn = S3.Bucket(BUCKET)

##  ingest raw csv and extract year and month (fill null with -1)
print("Reading in raw CCRB data")
ccrb_raw = pd.read_csv("s3://jdi-ccrb/raw/nyclu-misconduct-complaints.csv")
ccrb_raw["Year"] = pd.to_datetime(ccrb_raw["Incident Date"]).dt.year.fillna("-1").astype(int)
ccrb_raw["Month"] = pd.to_datetime(ccrb_raw["Incident Date"]).dt.month.fillna("-1").astype(int)
ccrb = ccrb_raw.copy()

##  load in precinct map and update ccrb as precincts and transit districts (fill others/null with -1)
pct_map = json.loads(S3.Bucket(BUCKET).Object('raw/nyclu-misconduct-complaints-precinct-mapping.json').get()['Body'].read().decode('utf-8'))
pcts = {}
for d in pct_map:
    pcts[d['Command'].strip()] = d['Complaints_Pct']
ccrb['Precinct'] = ccrb['Command'].replace(pcts)
ccrb['Precinct'] = np.where(ccrb['Precinct'].isin(pcts.values()), ccrb['Precinct'], '-1')

##  ingest census data and merge
print("Reading in census data")
census2010_raw = pd.read_csv("s3://jdi-ccrb/raw/census-2010-precinct-mapping.csv")
census2010 = preprocess_census.census(census2010_raw)
census2000 = pd.read_csv('s3://jdi-ccrb/tmp/census-2000-precinct-demographics.csv')
census = pd.merge(census2010, census2000, how='left', on='precinct_2020')
census["Precinct_Str"] = census["precinct_2020"].apply(lambda p: str(int(p)))
merged = ccrb.join(census.set_index("Precinct_Str"), how="left", on="Precinct")

##  linear interpolation of census
merged['Total_Population_Final'] = np.where(merged['Year']==2000, merged['Total_Population_00'], None)
merged['Total_Population_Final'] = np.where(merged['Year']==2010, merged['Total_Population'], merged['Total_Population_Final'])
merged['Black_Final'] = np.where(merged['Year']==2000, merged['Black_00'], None)
merged['Black_Final'] = np.where(merged['Year']==2010, merged['Black'], merged['Black_Final'])
merged['Hispanics_Final'] = np.where(merged['Year']==2000, merged['Hispanics_00'], None)
merged['Hispanics_Final'] = np.where(merged['Year']==2010, merged['Hispanics'], merged['Hispanics_Final'])
merged['NH_Asian_Final'] = np.where(merged['Year']==2000, merged['NH_Asian_00'], None)
merged['NH_Asian_Final'] = np.where(merged['Year']==2010, merged['NH_Asian'], merged['NH_Asian_Final'])
merged['NH_White_Final'] = np.where(merged['Year']==2000, merged['NH_White_00'], None)
merged['NH_White_Final'] = np.where(merged['Year']==2010, merged['NH_White'], merged['NH_White_Final'])
merged['Others_Final'] = np.where(merged['Year']==2000, merged['Others_00'], None)
merged['Others_Final'] = np.where(merged['Year']==2010, merged['Others_00'], merged['Others_Final'])
ccrb_grps = merged[['Year','Precinct','Total_Population_Final','Black_Final','Hispanics_Final','NH_Asian_Final','NH_White_Final','Others_Final']]
ccrb_grps = ccrb_grps[ccrb_grps['Year']>1990]
ccrb_grps = ccrb_grps.groupby('Precinct')
pcts = []
for grp in ccrb_grps:
    pct_lbl = grp[0]
    pct = grp[1]
    pct.Year = pd.to_datetime(pct.Year, format='%Y').dt.year
    pct = pct.drop_duplicates()
    pct = pct.sort_values(by='Year').set_index('Year').drop(columns=['Precinct'])
    pct.Total_Population_Final = pct.Total_Population_Final.astype(float)
    ref = []
    for col in pct.columns:
        pct[f"{col}"] = pct[f"{col}"].astype(float)
        tmp = pct[[f'{col}']]
        tmp[f"{col}"] = tmp[f"{col}"].interpolate(method = "spline", order = 1, limit_direction = "both")
        ref.append(tmp)
    fin = pd.concat(ref, axis=1)
    fin['Precinct'] = pct_lbl
    fin = fin.reset_index()
    pcts.append(fin)
out = pd.concat(pcts)
ccrb = pd.merge(merged, out, how='left', on=['Year','Precinct'])
for col in ccrb.columns:
    if col.endswith('_x'):
        del ccrb[f"{col}"]
    if col.endswith('_y'):
        ccrb[f'{col.rstrip("_y")}'] = ccrb[f"{col}"]
        del ccrb[f"{col}"]

##  collect large stops csvs from s3 and merge
print("Reading in stops data")
fns = [object_summary.key for object_summary in conn.objects.filter(Prefix=f"raw/nyclu-stops-")]
dfs = []

for fn in fns:
    print(f"Attempting:  {fn.split('/')[1]}")
    try: dfs.append(pd.read_csv(StringIO(S3.Bucket(BUCKET).Object(fn).get()["Body"].read().decode("utf-8"))))
    except: dfs.append(pd.read_csv(StringIO(S3.Bucket(BUCKET).Object(fn).get()["Body"].read().decode("iso-8859-1"))))

dfs_stripped = []

for df in dfs:
    try: df["pct"] = df["pct"].apply(lambda x: str(x).strip())
    except: df["STOP_LOCATION_PRECINCT"] = df["STOP_LOCATION_PRECINCT"].apply(lambda x: str(x).strip())
    dfs_stripped.append(df)

def month(s):
    if type(s)==int:
        if len(str(s))==7: return int(str(s)[0])
        elif len(str(s))==8: return int(str(s)[0:2])
    elif type(s)==str:
        if '-' in s: return int(s.split('-')[1])

for df in dfs_stripped:
    try: df['Month'] = df['datestop'].apply(lambda x: month(x))
    except: df['Month'] = pd.to_datetime(df["STOP_FRISK_DATE"]).dt.month.fillna("-1").astype(int)            

cts_dfs = []

for df in dfs_stripped:
    try:
        df["year"] = df["year"].apply(lambda x: str(x).strip())
        df = df[df["year"] != ""]
        cts_dfs.append(pd.DataFrame(df.groupby(["year", "Month"]).size(), columns=["MONTH_STOPS"]).reset_index().rename(columns={"year": "Year"}))
    except:
        df["YEAR2"] = df["YEAR2"].astype(int)
        if df["YEAR2"][0] == 2017.0: df["YEAR2"] = 2017
        df = df[df["YEAR2"] != ""]
        cts_dfs.append(pd.DataFrame(df.groupby(["YEAR2", "Month"]).size(), columns=["MONTH_STOPS"]).reset_index().rename(columns={"YEAR2": "Year"}))

cts_df = pd.concat(cts_dfs).reset_index().drop(columns=["index"])
cts_df["Year"] = cts_df["Year"].astype(int)
cts_df["Month"] = cts_df["Month"].astype(int)

pct_cts_dfs = []

for df in dfs_stripped:
    try:
        df["year"] = df["year"].apply(lambda x: str(x).strip())
        df = df[df["year"] != ""]
        pct_cts_dfs.append(pd.DataFrame(df.groupby(["pct", "year", "Month"]).size(), columns=["PCT_MONTH_STOPS"]).reset_index().rename(columns={"pct": "Precinct", "year": "Year"}))
    except:
        df["YEAR2"] = df["YEAR2"].astype(int)
        if df["YEAR2"][0] == 2017.0: df["YEAR2"] = 2017
        df = df[df["YEAR2"] != ""]
        pct_cts_dfs.append(pd.DataFrame(df.groupby(["STOP_LOCATION_PRECINCT", "YEAR2", "Month"]).size(), columns=["PCT_MONTH_STOPS"]).reset_index().rename(columns={"STOP_LOCATION_PRECINCT": "Precinct", "YEAR2": "Year"}))

pct_cts_df = pd.concat(pct_cts_dfs).reset_index().drop(columns=["index"])
pct_cts_df["Year"] = pct_cts_df["Year"].astype(int)
pct_cts_df["Month"] = pct_cts_df["Month"].astype(int)
pct_cts_df["Precinct"] = np.where(pct_cts_df["Precinct"].isin([" ", "#NULL!", "208760", "999", ""]), "999", pct_cts_df["Precinct"])
ccrb = pd.merge(ccrb, cts_df, how="left", on=["Year","Month"])
ccrb = pd.merge(ccrb, pct_cts_df, how="left", on=["Precinct", "Year","Month"])

pct_cts_dfs = []
cts_df = pd.DataFrame(list(zip(range(2003, 2020), [len(df) for df in dfs_stripped])), columns=["Year", "YR_STOPS"])

for df in dfs_stripped:
    try:
        df["year"] = df["year"].apply(lambda x: str(x).strip())
        df = df[df["year"] != ""]
        pct_cts_dfs.append(pd.DataFrame(df.groupby(["pct", "year"]).size(), columns=["PCT_YR_STOPS"]).reset_index().rename(columns={"pct": "Precinct", "year": "Year"}))
    except:
        df["YEAR2"] = df["YEAR2"].astype(int)
        if df["YEAR2"][0] == 2017.0: df["YEAR2"] = 2017
        df = df[df["YEAR2"] != ""]
        pct_cts_dfs.append(pd.DataFrame(df.groupby(["STOP_LOCATION_PRECINCT", "YEAR2"]).size(), columns=["PCT_YR_STOPS"]).reset_index().rename(columns={"STOP_LOCATION_PRECINCT": "Precinct", "YEAR2": "Year"}))

pct_cts_df = pd.concat(pct_cts_dfs).reset_index().drop(columns=["index"])
pct_cts_df["Year"] = pct_cts_df["Year"].astype(int)
pct_cts_df["Precinct"] = np.where(pct_cts_df["Precinct"].isin([" ", "#NULL!", "208760", "999", ""]), "999", pct_cts_df["Precinct"])
ccrb = pd.merge(ccrb, cts_df, how="left", on="Year")
ccrb = pd.merge(ccrb, pct_cts_df, how="left", on=["Precinct", "Year"])

##  ingest number of officers and merge
print("Reading in number of officers data")
off = pd.read_csv("s3://jdi-ccrb/raw/kaplan-police.csv")
off = off[["year", "population", "total_employees_officers", "total_employees_total"]].rename(columns={"year": "Year", "population": "YR_CITY_POP", "total_employees_officers": "YR_NUM_OFFICERS", "total_employees_total": "YR_NUM_NYPD_EMPLOYEES"})
ccrb = pd.merge(ccrb, off, how="left", on="Year")

##  ingest arrest count and merge
print("Reading in number of arrests data")
arr = pd.read_csv("s3://jdi-ccrb/raw/kaplan-arrests.csv")
arr = arr[["year", "all_arrests_total_tot_arrests"]].rename(columns={"year": "Year", "all_arrests_total_tot_arrests": "YR_ARRESTS"})
ccrb = pd.merge(ccrb, arr, how="left", on="Year")

##  ingest offense count and merge
print("Reading in number of offenses data")
crm = pd.read_csv("s3://jdi-ccrb/raw/kaplan-offenses.csv")
crm = crm[["year", "actual_all_crimes", "tot_clr_all_crimes"]].rename(columns={"year": "Year", "actual_all_crimes": "YR_OFFENSES", "tot_clr_all_crimes": "YR_OFFENSES_CLEARED"})
ccrb = pd.merge(ccrb, crm, how="left", on="Year")

##  save tmp to csv on s3
ccrb.to_csv("s3://jdi-ccrb/tmp/ccrb-minus-crime-complaints.csv", index=False)