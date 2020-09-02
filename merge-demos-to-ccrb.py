import pandas as pd
import re
df = pd.read_csv("raw/CCRB_database_raw.csv")
precincts = set(filter(lambda x: re.match("(\d+)\sPCT", x), df["Command"]))
pdf = df[df["Command"].isin(precincts)].copy()
pdf["Precinct"] = pdf["Command"].apply(lambda c: re.search("(\d+)", c).group(1))
census = pd.read_csv("precincts_demos.csv")

census["Precinct_Str"] = census["precinct_2020"].apply(lambda p: str(int(p)).zfill(3))
merged = pdf.join(census.set_index("Precinct_Str"), on="Precinct")

merged['Year'] = pd.to_datetime(merged['Incident Date']).dt.year.fillna('-1').astype(int)
merged['Precinct'] = merged['Precinct'].astype(int)
incidents = pd.read_csv('./Incidents.csv')
merged = pd.merge(merged, incidents, how='left', left_on=['Year', 'Precinct'], right_on=['YEAR', 'ADDR_PCT_CD']).drop(labels=['YEAR', 'ADDR_PCT_CD'], axis=1)

merged.to_csv("ccrb_merged.csv", index=False)
