# %%
import numpy as np
import pandas as pd

# %%
complaints = pd.read_csv('./NYPD_Complaint_Data_Historic.csv', parse_dates=True)

# %%
cdf = complaints.copy()
cdf['YEAR'] = pd.to_datetime(cdf['CMPLNT_FR_DT'], errors="coerce").dt.year
cdf = cdf[cdf['YEAR'] >= 1980]
otypes = pd.read_csv('./offensetypes.csv')
cdf = pd.merge(cdf, otypes[['OFNS_DESC', 'OFNS_TYPE']], how='left', on='OFNS_DESC')
table = pd.pivot_table(cdf, index=['YEAR', 'ADDR_PCT_CD'], columns=['OFNS_TYPE'], aggfunc='size', fill_value=0)
table = table.reset_index()
table[['YEAR', 'ADDR_PCT_CD']] = table[['YEAR', 'ADDR_PCT_CD']].astype(int)
table.to_csv("Incidents.csv", index=False)
