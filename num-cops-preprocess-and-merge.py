import pandas as pd
import boto3
from io import StringIO

off = pd.read_csv("jacobdkaplan.com_police_count_New York City Police Department_New York.csv")
off = off[["year","population","total_employees_officers","total_employees_total"]].rename(columns={"year":"Year","population":"YR_CITY_POP","total_employees_officers":"YR_NUM_OFFICERS","total_employees_total":"YR_NUM_NYPD_EMPLOYEES"})

ccrb = pd.read_csv("ccrb_merged.csv")

if "YR_NUM_OFFICERS" not in ccrb.columns: 
    ccrb = pd.merge(ccrb, off, how='left', on="Year")
    ccrb.to_csv("ccrb_merged.csv", index=False)
else: print(f"NUM OFFICERS already merged")
