import pandas as pd
import re
df = pd.read_csv("raw/CCRB_database_raw.csv")
precincts = set(filter(lambda x: re.match("(\d+)\sPCT", x), df["Command"]))
pdf = df[df["Command"].isin(precincts)].copy()
pdf["Precinct"] = pdf["Command"].apply(lambda c: re.search("(\d+)", c).group(1))
census = pd.read_csv("precincts_demos.csv")

census["Precinct_Str"] = census["precinct_2020"].apply(lambda p: str(int(p)).zfill(3))
merged = pdf.join(census.set_index("Precinct_Str"), on="Precinct")
merged.to_csv("ccrb_demos.csv", index=False)
