# Analysis of NYPD Officer Misconduct Complaint Data
### NYU <a href="https://publicsafetylab.org/"><b>Public Safety Lab</b></a>

In the wake of George Floyd's death at the hands of officers of the Minneapolis Police Department, New York State repealed Section 50-a of its civil service law, permitting law enforcement agencies to shield personnel records from public disclosure. 

On August 20, 2020, the New York Civil Liberties Union released a <a href="https://github.com/new-york-civil-liberties-union/NYPD-Misconduct-Complaint-Database">dataset</a> of 323,911 misconduct complaints filed against officers of the City of New York Police Department (NYPD) with the Civilian Complaint Review Board (CCRB). The dataset includes complaints with incident dates ranging from August 7, 1978 to May 24, 2020. In addition to incident dates, the data include officer identifiers, the commands to which officers were assigned, and CCRB dispositions.

This repository contains the data and code used in the Public Safety Lab's analyis of these misconduct data. 

## Repository Structure
Note: Many of the raw and temporary files referenced are stored in a public S3 bucket (e.g., the complete output data is stored at <a href='https://psl-ccrb.s3.amazonaws.com/out/data.csv'>https://psl-ccrb.s3.amazonaws.com/out/data.csv</a>).

The <a href="https://github.com/publicsafetylab/PSL-CCRB/blob/master/process.py">process.py</a> script reads in the raw CCRB data and merges the following additional data sources:
<ul>
  <li>2010 U.S. Census data, mapped to 2020 NYPD precincts by <a href="https://johnkeefe.net/nyc-police-precinct-and-census-data">John Keefe</a></li>
  <li>Annual counts of the numbers of NYPD employees, officers, arrests, offenses, and cleared offenses, compiled by <a href="https://jacobdkaplan.com">Jacob Kaplan</a></li>
  <li>The NYPD stop-and-frisk <a href="https://www1.nyc.gov/site/nypd/stats/reports-analysis/stopfrisk.page">dataset</a></li>
  <li>The NYPD crime complaints dataset from <a href="https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i">NYC Open Data</a></li>
</ul>
  
The final data is batched into 6 files in the <a href="https://github.com/publicsafetylab/PSL-CCRB/tree/master/data">data</a> directory (data_chunk_0 thru data_chunk_5).

The <a href="https://github.com/publicsafetylab/PSL-CCRB/blob/master/visualize.py">visualize.py</a> script reads in the processed CCRB data, flattens per precinct-year and per precinct count means for various features, and creates visualizations corresponding to the figures in the NYPD Officer Misconduct Analysis report.

## Contact Information

Please contact Public Safety Lab Director Anna Harvey or Lead Data Scientist Orion Taylor (<a href="https://publicsafetylab.org/who-we-are"><b>WHO WE ARE</b></a>) with questions, comments and feedback.
