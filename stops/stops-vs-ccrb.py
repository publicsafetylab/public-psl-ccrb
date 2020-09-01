import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime

# OJT: for later, to fill in any missing dates with 0.0 count
idx = pd.DataFrame(pd.date_range('2019-01-01', '2019-12-31'), columns=['date'])

stops = pd.read_csv("raw/sqf-2019.csv")
stops["STOP_LOCATION_PRECINCT"] = stops["STOP_LOCATION_PRECINCT"].apply(lambda x: str(x).zfill(3))
stops["STOP_LOCATION_PRECINCT"] = stops["STOP_LOCATION_PRECINCT"].astype(str) + " PCT"
# OJT: only looking at precinct and date for now
stops = stops[["STOP_LOCATION_PRECINCT", "STOP_FRISK_DATE"]]

stop_count = stops.groupby(["STOP_LOCATION_PRECINCT", "STOP_FRISK_DATE"]).size().reset_index(name="stop_count")
stop_count = stop_count.rename(columns={"STOP_LOCATION_PRECINCT":"precinct", "STOP_FRISK_DATE":"date", "stop_count":"num_stops"})
stop_count["date"] = stop_count["date"].apply(lambda x: datetime.strptime(x, "%m/%d/%y"))
stop_count["num_stops"] = stop_count["num_stops"].astype(float)
stop_count = stop_count.groupby('date').sum().reset_index()
stop_count = pd.merge(idx, stop_count, how='left', left_on='date', right_on='date')
stop_count['num_stops'] = np.where(stop_count['num_stops'].isnull(), 0.0, stop_count['num_stops'])

complaints = pd.read_csv("raw/CCRB_database_raw.csv")
# OJT: subsetting to 2019 precinct-only
complaints = complaints[(complaints["Incident Date"].notnull()) & (complaints["Incident Date"].str.endswith("/2019"))]
complaints = complaints[(complaints["Command"].str.endswith(" PCT")) & (complaints["Command"].str[0].str.isdigit())]

complaint_count = complaints.groupby(["Incident Date", "Command"]).size().reset_index(name="complaint_count")
complaint_count = complaint_count.rename(columns={"Command":"precinct", "Incident Date":"date", "complaint_count":"num_complaints"})
complaint_count["date"] = complaint_count["date"].apply(lambda x: datetime.strptime(x, "%m/%d/%Y"))
complaint_count["num_complaints"] = complaint_count["num_complaints"].astype(float)
complaint_count = complaint_count.groupby('date').sum().reset_index()
complaint_count = pd.merge(idx, complaint_count, how='left', left_on='date', right_on='date')
complaint_count['num_complaints'] = np.where(complaint_count['num_complaints'].isnull(), 0.0, complaint_count['num_complaints'])

# OJT: package per-day allegation and stop counts
city = pd.merge(complaint_count, stop_count, on='date')
city.to_csv('output/complaints-stops-per-day.csv', index=False)