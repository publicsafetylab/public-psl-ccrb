# <a href="https://publicsafetylab.org/jail-data-initiative"><b>Jail Data Initiative</b></a>: Analysis of Data from the New York City Civilian Complaint Review Board (CCRB)
### NYU <a href="https://publicsafetylab.org/"><b>Public Safety Lab</b></a>

On August 20, 2020 the New York Civil Liberties Union (NYCLU) <a href="https://www.nyclu.org/en/press-releases/nyclu-makes-35-years-nypd-misconduct-data-available-public">announced</a> its public release of a <a href="https://github.com/new-york-civil-liberties-union/NYPD-Misconduct-Complaint-Database">dataset</a> of 300,000+ allegations of misconduct against NYPD officers filed by civilians to the CCRB. The dataset features civilian complaints and their component allegations, with incident dates ranging from August 7, 1978 to May 24, 2020 (one day prior to the killing of George Floyd). They include officer-identifying information, incident date, precinct, allegation and allegation type, CCRB and NYPD dispositions (with date of most recent modification), and penalty description.

## Repository Structure
This repository contains the files and directories outlined below. Many of the raw and temporary files referenced are stored in a public S3 bucket (e.g., the complete output data is stored at <a href='https://jdi-ccrb.s3.amazonaws.com/out/data.csv'>https://jdi-ccrb.s3.amazonaws.com/out/data.csv</a>).
<ul>
  <li><a href="https://github.com/publicsafetylab/JDI-CCRB/blob/master/preprocess_census.py">preprocess_census.py</a> processes raw U.S Census data from 2000 and 2010, with subprocesses and data stored in the <a href="https://github.com/publicsafetylab/JDI-CCRB/tree/master/raw">raw</a> directory
  </ul>

## Contact Information

Please contact Public Safety Lab Director Anna Harvey or Lead Data Scientist Orion Taylor (<a href="https://publicsafetylab.org/who-we-are"><b>WHO WE ARE</b></a>) with questions, comments and feedback.
