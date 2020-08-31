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

# %%
# load all complaints and precinct demographics
df = pd.read_excel('./CCRB_database_raw.xlsx')
precincts  = pd.read_csv('./nyc_2010pop_2020precincts.csv')

# %%
# filter complaints in a numbered precinct
mask = df['Command'].str.strip(' PCT').str.isnumeric().astype(bool)
dfpct = df[mask]
dfpct['Precinct'] = dfpct['Command'].str.extract(r'(\d+)')[0].dropna().astype(int)

# %%
# metrics by year - all complaints
df['Substantiated'] = df['Board Disposition'].str.contains('Substantiated')
byyear = pd.DataFrame()
grouped = df.groupby(df['Incident Date'].dt.year)
byyear['Total'] = grouped.size()
byyear['Substantiated'] = grouped['Substantiated'].sum()
byyear['Pct Subst'] = byyear['Substantiated'] / byyear['Total']

# %%
# metrics by officer - all complaints
byofficer = pd.DataFrame()
grouped = df.groupby(['First Name', 'Last Name'])
byofficer['Total'] = grouped.size()
byofficer['Substantiated'] = grouped['Substantiated'].sum()
byofficer['Pct Subst'] = byofficer['Substantiated'] / byofficer['Total']