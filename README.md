# <a href="https://publicsafetylab.org/jail-data-initiative"><b>Jail Data Initiative</b></a>: Analysis of Data from the New York City Civilian Complaint Review Board (CCRB)
### NYU <a href="https://publicsafetylab.org/"><b>Public Safety Lab</b></a>

On August 20, 2020 the New York Civil Liberties Union (NYCLU) <a href="https://www.nyclu.org/en/press-releases/nyclu-makes-35-years-nypd-misconduct-data-available-public">announced</a> its public release of a <a href="https://github.com/new-york-civil-liberties-union/NYPD-Misconduct-Complaint-Database">dataset</a> of 300,000+ allegations of misconduct against NYPD officers filed by civilians to the CCRB. The dataset features civilian complaints and their component allegations, with incident dates ranging from August 7, 1978 to May 24, 2020 (one day prior to the killing of George Floyd). They include officer-identifying information, incident date, precinct, allegation and allegation type, CCRB and NYPD dispositions (with date of most recent modification), and penalty description.

## Repository Structure
Note: Many of the raw and temporary files referenced are stored in a public S3 bucket (e.g., the complete output data is stored at <a href='https://jdi-ccrb.s3.amazonaws.com/out/data.csv'>https://jdi-ccrb.s3.amazonaws.com/out/data.csv</a>).

The <a href="">process.py</a> script reads in the raw NYCLU NYC CCRB data and merges the following additional data sources:
<ul>
  <li>2010 U.S. Census data, mapped to 2020 NYPD precincts by <a href="https://johnkeefe.net/nyc-police-precinct-and-census-data">John Keefe</a></li>
  <li>Annual counts of the numbers of NYPD employees/officers, NYC arrests, and NYC offenses/cleared offenses, compiled by <a href="https://jacobdkaplan.com">Jacob Kaplan</a></li>
  <li>The NYPD stop-and-frisk <a href="https://www1.nyc.gov/site/nypd/stats/reports-analysis/stopfrisk.page">dataset</a></li>
  <li>The NYPD crime complaints dataset from <a href="https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i">NYC Open Data</a></li>
</ul>
  
The final data is batched into 6 files in the <a href="https://github.com/publicsafetylab/JDI-CCRB/tree/master/data">data</a> directory (data_chunk_0 thru data_chunk_6).

## Contact Information

Please contact Public Safety Lab Director Anna Harvey or Lead Data Scientist Orion Taylor (<a href="https://publicsafetylab.org/who-we-are"><b>WHO WE ARE</b></a>) with questions, comments and feedback.
