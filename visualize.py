from plotly.subplots import make_subplots
import plotly.graph_objs as go
from itertools import product
import plotly.express as px
from io import StringIO
import seaborn as sns
import pandas as pd
import numpy as np
import warnings
import boto3
import json
import os

# Check if viz directory exists
if not os.path.isdir("viz"):
    os.mkdir("viz")

# Connect to public AWS S3 bucket
# Hosted by NYU's Public Safety Lab @ https://psl-ccrb.s3.amazonaws.com/
S3 = boto3.resource("s3")
BUCKET = "psl-ccrb"
print(f"Connecting to NYU Public Safety Lab AWS S3 bucket {BUCKET}")
conn = S3.Bucket(BUCKET)

# Import NYU PSL NYC CCRB processed data, and separate substantiated complaints
ccrb = pd.read_csv("s3://psl-ccrb/out/data.csv")
ccrb["Num_NYPD_Officers_Year"] = np.where(ccrb["Year"]==2003, 36700, ccrb["Num_NYPD_Officers_Year"])

# Function to sum report crime column counts to produce per-metric reported crimes
def sum_crimes(df):
    cols = [c for c in df.columns if "Num_Crime_Complaints_" in c and "_Precinct_Year" in c]
    df = df[["Year", "Precinct"] + cols].drop_duplicates().dropna()
    df["Crime Reports"] = df[cols].sum(axis=1)
    return df[["Year", "Precinct", "Crime Reports"]]

# Function to flatten to means by precinct-year and precinct
def compile_precincts(dfa):
    # rename demographic columns
    for c in dfa.columns:
        if "NH_" in c:
            dfa = dfa.rename(columns={c: f"Non-Hispanic {c.split('_',1)[1]}"})
    
    # exclusion criteria
    dfa = dfa[(dfa["Year"] >= 2006) & (dfa["Year"] <= 2019) & (dfa["Precinct"] != "-1")]
    
    # clean out Precinct 121 < 2014 (not yet an official precinct)
    pct121 = dfa[dfa["Precinct"] == "121"]
    pct121_del = pct121[pct121["Year"] < 2014] 
    dfa = dfa[~dfa.isin(pct121_del)]
    dfa = dfa[dfa["Year"].notna()]
    
    # substantiated DataFrame
    dfs = dfa[dfa["Board Disposition"].str.contains("Substantiated ")]
    
    # set up all combinations by precinct-year to fill missing later
    precincts = list(set(dfa.Precinct.values))
    years = list(set(dfa.Year.values))
    blanks = pd.DataFrame(list(product(years, precincts)), columns=["Year", "Precinct"])
    pct121 = blanks[blanks["Precinct"] == "121"]
    pct121_del = pct121[pct121["Year"] < 2014] 
    blanks = blanks[~blanks.isin(pct121_del)]
    blanks = blanks[blanks["Year"].notna()]
    
    # collect number of reported crimes all types by precinct-year (spot fill missing TD11)
    cg = sum_crimes(dfa)
    td11_2017_crimes = {"Year": 2017, "Precinct": "TD11", "Crime Reports": 897.0}
    cg = cg.append(td11_2017_crimes, ignore_index=True)
    
    # collect arrests per precinct-year and per-precinct
    ayg = dfa.drop_duplicates(["Precinct", "Year", "Arrests_Precinct_Year"])[["Precinct", "Year", "Arrests_Precinct_Year"]]
    apg = ayg.groupby("Precinct")["Arrests_Precinct_Year"].mean().reset_index().rename(columns={"Arrests_Precinct_Year": "Annual_Mean_Arrests"})

    # for relevant demographics, collect per-precinct proportions
    demos = ["Black", "Non-Hispanic Asian", "Non-Hispanic White"]
    demo_dfs = []
    for demo in demos:
        dfa[f"{demo}_Percent"] = 100 * dfa[f"{demo}_Percent"]
        demo_dfs.append(dfa.drop_duplicates(["Precinct", f"{demo}_Percent"])[["Precinct", f"{demo}_Percent"]].rename(columns={f"{demo}_Percent": f"2010_Percent_{demo.split('_')[0]}_Residents"}).sort_values(by="Precinct"))
    demo_df = pd.concat(demo_dfs, axis=1)
    demo_df = demo_df.loc[:,~demo_df.columns.duplicated()]
    
    # merge reported crimes, demographics into all precinct-year combinations
    pyg = pd.merge(blanks, cg, how="left", on=["Precinct", "Year"])
    pyg = pd.merge(pyg, demo_df, how="left", on="Precinct")
    
    # collect all complaints, substantiated complaints per precinct-year
    complaints = dfa.groupby(["Year", "Precinct"])["Unique Id"].count().reset_index()
    substantiated = dfs.groupby(["Year", "Precinct"])["Unique Id"].count().reset_index()
    
    # merge in complaint counts per precinct-year
    pyg = pd.merge(pyg, complaints, how="left", on=["Year", "Precinct"]).rename(columns={"Unique Id": "Complaints"})
    pyg["Complaints"] = pyg["Complaints"].fillna(0.0)
    pyg = pd.merge(pyg, substantiated, how="left", on=["Year", "Precinct"]).rename(columns={"Unique Id": "Substantiated"})
    pyg["Substantiated"] = pyg["Substantiated"].fillna(0.0)
    
    # collect means of relevant columns
    pyg = pd.merge(pyg, pyg.groupby("Precinct")["Crime Reports"].mean().reset_index().rename(columns={"Crime Reports": "Annual_Mean_Crime_Reports"}), on="Precinct")
    pyg = pd.merge(pyg, pyg.groupby("Precinct")["Complaints"].mean().reset_index().rename(columns={"Complaints": "Annual_Mean_Complaints"}), on="Precinct")
    pyg = pd.merge(pyg, pyg.groupby("Precinct")["Substantiated"].mean().reset_index().rename(columns={"Substantiated": "Annual_Mean_Substantiated"}), on="Precinct")
    pyg = pd.merge(pyg, ayg, how="left", on=["Year", "Precinct"])
    
    # save precinct-year flat file to CSV on S3 and in out directory
    pyg.to_csv("s3://psl-ccrb/out/data-flat-by-precinct-year.csv", index=False)
    pyg.to_csv("out/data-flat-by-precinct-year.csv", index=False)

    # collect unique number of officers per precinct
    og = dfa.groupby("Precinct")["Unique Id"].nunique().reset_index().rename(columns={"Unique Id": "Officers"})

    # group by precinct and collect complaints/substantiated per officer
    pg = pyg.drop(["Year", "Crime Reports", "Complaints", "Substantiated", "Arrests_Precinct_Year"], axis=1).drop_duplicates().sort_values(by="Precinct")
    pg = pd.merge(pg, pyg.groupby("Precinct")["Complaints"].sum().reset_index(), on="Precinct")
    pg = pd.merge(pg, pyg.groupby("Precinct")["Substantiated"].sum().reset_index(), on="Precinct")
    pg = pd.merge(pg, og, how="left", on="Precinct")
    pg["Mean_Complaints_per_Officer"] = pg["Complaints"]/pg["Officers"]
    pg["Mean_Substantiated_per_Officer"] = pg["Substantiated"]/pg["Officers"]
    pg = pd.merge(pg, apg, how="left", on=["Precinct"])
    
    # save precinct flat file to CSV on S3 and in out directory
    pg.to_csv("s3://psl-ccrb/out/data-flat-by-precinct.csv", index=False)
    pg.to_csv("out/data-flat-by-precinct.csv", index=False)

    return pg

# Function to generate confidence interval shape in seaborn to pass to plotly
def seaborn_conf_int(df, x, y):
    rg = sns.regplot(x=df[x], y=df[y])
    X = rg.get_lines()[0].get_xdata()
    Y = rg.get_lines()[0].get_ydata()
    P = rg.get_children()[1].get_paths()
    p_codes = {1: "M", 2: "L", 79: "Z"}
    path = ""
    for s in P[0].iter_segments():
        c = p_codes[s[1]]
        xx, yy = s[0]
        path += c + str("{:.5f}".format(xx)) + " " + str("{:.5f}".format(yy))
    shapes = [dict(type="path", path=path, line=dict(width=0.1,color="rgba(68, 122, 219, 0.25)"), fillcolor="rgba(68, 122, 219, 0.25)")]  
    return shapes

# Function to generate Figure 1
def annual_complaints(dfa, start, stop, figno, ign_pcts=[]):
    dfa = dfa[(dfa["Year"] >= start) & (dfa["Year"] <= stop)]
    dfa = dfa[~dfa["Precinct"].isin(ign_pcts)]
    dfs = dfa[dfa["Board Disposition"].str.contains("Substantiated ")]
    ga = dfa.groupby("Year")["Unique Id"].count().reset_index().rename(columns={"Unique Id": "All"})
    gb = dfs.groupby("Year")["Unique Id"].count().reset_index().rename(columns={"Unique Id": "Substantiated"})
    g = pd.merge(ga, gb, on="Year")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=g.Year, y=g.All, name="All Complaints"),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=g.Year, y=g.Substantiated, name="Substantiated"),
        secondary_y=True,
    )
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Number of Misconduct Complaints vs. Number of Substantiated Misconduct Complaints ({start}-{stop})",
            'y':0.9,
            'x':0.475,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.update_xaxes(title_text="<span style='font-size: 12px;'>Year</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Number of Misconduct Complaints</span>", secondary_y=False)
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Number of Substantiated Complaints</span>", secondary_y=True)
    fig.update(layout_showlegend=False)
    fig.show()
    
    return g

# Function to generate Figure 2
def annual_complaints_officers_crimes(dfa, start, stop, figno, ign_pcts=[]):
    dfa = dfa[(dfa["Year"] >= start) & (dfa["Year"] <= stop)]
    dfa = dfa[~dfa["Precinct"].isin(ign_pcts)]
    dfs = dfa[dfa["Board Disposition"].str.contains("Substantiated ")]
    ga = dfa.groupby("Year")["Unique Id"].count().reset_index().rename(columns={"Unique Id": "All"})
    gb = dfs.groupby("Year")["Unique Id"].count().reset_index().rename(columns={"Unique Id": "Substantiated"})
    g = pd.merge(ga, gb, on="Year")
    og = dfa.drop_duplicates(["Year", "Num_NYPD_Officers_Year"])[["Year", "Num_NYPD_Officers_Year"]].sort_values(by="Year")
    cg = dfa.drop_duplicates(["Year", "Num_Offenses_Year"])[["Year", "Num_Offenses_Year"]].sort_values(by="Year")
    g = pd.merge(g, og, on="Year")
    g = pd.merge(g, cg, on="Year")
    
    fig = make_subplots(rows=2, cols=2)
    fig.add_trace(
        go.Scatter(x=g.Year, y=g.All, name="All Complaints"),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=g.Year, y=g.Substantiated, name="Substantiated"),
        row=1, col=2
    )
    fig.add_trace(
        go.Scatter(x=g.Year, y=g.Num_NYPD_Officers_Year, name="NYPD Officers"),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(x=g.Year, y=g.Num_Offenses_Year, name="Crime Reports"),
        row=2, col=2
    )
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Numbers of Misconduct Complaints, Substantiated Misconduct Complaints, Sworn NYPD Officers & Reported Crimes ({start}-{stop})",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.update_xaxes(title_text="<span style='font-size: 12px;'>Year</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>All Misconduct Complaints</span>", row=1, col=1)
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Substantiated Complaints</span>", row=1, col=2)
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Sworn NYPD Officers</span>", row=2, col=1)
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Reported Crimes</span>", row=2, col=2)
    fig.update(layout_showlegend=False)
    fig.show()
    
    return g

# Function to generate Figure 3
def annual_complaints_vs_officers_reg(dfa, start, stop, figno, ign_pcts=[]):
    dfa = dfa[(dfa["Year"] >= start) & (dfa["Year"] <= stop)]
    dfa = dfa[~dfa["Precinct"].isin(ign_pcts)]
    g = dfa.groupby("Year")["Unique Id"].count().reset_index().rename(columns={"Unique Id": "Complaints"})
    og = dfa.drop_duplicates(["Year", "Num_NYPD_Officers_Year"])[["Year", "Num_NYPD_Officers_Year"]].sort_values(by="Year")
    g = pd.merge(g, og, on="Year")
    g = g.rename(columns={"Num_NYPD_Officers_Year": "NYPD Officers"})
    
    shapes = seaborn_conf_int(g, "NYPD Officers", "Complaints") 
    fig = px.scatter(g, x=g["NYPD Officers"], y=g.Complaints, color=g.Year, text=g.Year, trendline="ols")
    fig.update_traces(textposition='top center', textfont_size=6)
    fig.update_layout(shapes=shapes)
    fig.update_xaxes(title_text="<span style='font-size: 12px;'>Number of Sworn NYPD Officers</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Number of Misconduct Complaints</span>")
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Number of Misconduct Complaints vs. Number of Sworn NYPD Officers ({start}-{stop})",
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.show()
    
    results = px.get_trendline_results(fig)
    return g, results.px_fit_results.iloc[0].summary()

# Function to generate Figure A1
def annual_subst_complaints_vs_officers_reg(dfa, start, stop, figno, ign_pcts=[]):
    dfa = dfa[(dfa["Year"] >= start) & (dfa["Year"] <= stop)]
    dfa = dfa[~dfa["Precinct"].isin(ign_pcts)]
    dfs = dfa[dfa["Board Disposition"].str.contains("Substantiated ")]
    g = dfs.groupby("Year")["Unique Id"].count().reset_index().rename(columns={"Unique Id": "Substantiated"})
    og = dfa.drop_duplicates(["Year", "Num_NYPD_Officers_Year"])[["Year", "Num_NYPD_Officers_Year"]].sort_values(by="Year")
    g = pd.merge(g, og, on="Year")
    g = g.rename(columns={"Num_NYPD_Officers_Year": "NYPD Officers"})

    shapes = seaborn_conf_int(g, "NYPD Officers", "Substantiated") 
    fig = px.scatter(g, x=g["NYPD Officers"], y=g.Substantiated, color=g.Year, text=g.Year, trendline="ols")
    fig.update_traces(textposition='top center', textfont_size=6)
    fig.update_layout(shapes=shapes)
    fig.update_xaxes(title_text="<span style='font-size: 12px;'>Number of Sworn NYPD Officers</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Number of Substantiated Misconduct Complaints</span>")
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Number of Substantiated Misconduct Complaints vs. Number of Sworn NYPD Officers ({start}-{stop})",
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.show()
    
    results = px.get_trendline_results(fig)
    return g, results.px_fit_results.iloc[0].summary()

# Function to generate Figure 4
def annual_complaints_vs_reported_crime_reg(df, start, stop, figno, ign_pcts=[]):
    df = df.rename(columns={"Annual_Mean_Crime_Reports": "Mean Annual Reported Crimes", "Annual_Mean_Complaints": "Mean Annual Misconduct Complaints"})
    
    shapes = seaborn_conf_int(df, "Mean Annual Reported Crimes", "Mean Annual Misconduct Complaints") 
    fig = px.scatter(df, x=df["Mean Annual Reported Crimes"], y=df["Mean Annual Misconduct Complaints"], text=df.Precinct, trendline="ols")
    fig.update_traces(textposition='top center', textfont_size=6)
    fig.update_layout(shapes=shapes)
    fig.update_xaxes(title_text="<span style='font-size: 12px;'>Mean Annual Number of Reported Crimes</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Mean Annual Number of Misconduct Complaints</span>")
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Per-Precinct Mean Annual Misconduct Complaints vs. Mean Annual Reported Crimes ({start}-{stop})",
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.show()
    
    results = px.get_trendline_results(fig)
    global b0, b1
    b0, b1 = results.px_fit_results.iloc[0].params
    return df, results.px_fit_results.iloc[0].summary()

# Function to generate Figure A2
def annual_subst_complaints_vs_reported_crime_reg(df, start, stop, figno, ign_pcts=[]):
    df = df.rename(columns={"Annual_Mean_Crime_Reports": "Mean Annual Reported Crimes", "Annual_Mean_Substantiated": "Mean Annual Substantiated Misconduct Complaints"})
    
    shapes = seaborn_conf_int(df, "Mean Annual Reported Crimes", "Mean Annual Substantiated Misconduct Complaints") 
    fig = px.scatter(df, x=df["Mean Annual Reported Crimes"], y=df["Mean Annual Substantiated Misconduct Complaints"], text=df.Precinct, trendline="ols")
    fig.update_traces(textposition='top center', textfont_size=6)
    fig.update_layout(shapes=shapes)
    fig.update_xaxes(title_text="<span style='font-size: 12px;'>Mean Annual Number of Reported Crimes</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Mean Annual Number of Substantiated Misconduct Complaints</span>")
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Per-Precinct Mean Annual Substantiated Misconduct Complaints vs. Mean Annual Reported Crimes ({start}-{stop})",
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.show()
    
    results = px.get_trendline_results(fig)
    global b0s, b1s
    b0s, b1s = results.px_fit_results.iloc[0].params
    return df, results.px_fit_results.iloc[0].summary()

# Function to generate Figures 5, A6, A7
def annual_complaints_vs_prop_demo_reg(df, start, stop, figno, demo, ign_pcts=[]):
    df = df.copy()
    df = df[df[f"2010_Percent_{demo}_Residents"].notna()]
    df["Precinct"] = df["Precinct"].astype(int)
    df = df.rename(columns={f"2010_Percent_{demo}_Residents": f"2010 Percent {demo} Residents", "Annual_Mean_Complaints": "Mean Annual Misconduct Complaints"})
    df["Annual_Mean_Complaints_Pred"] = b0 + b1 * df["Annual_Mean_Crime_Reports"]
    df["Mean Annual 'Excess' Complaints"] = df["Mean Annual Misconduct Complaints"] - df["Annual_Mean_Complaints_Pred"]
    
    shapes = seaborn_conf_int(df, f"2010 Percent {demo} Residents", "Mean Annual 'Excess' Complaints") 
    fig = px.scatter(df, x=df[f"2010 Percent {demo} Residents"], y=df["Mean Annual 'Excess' Complaints"], color=df.Precinct, text=df.Precinct, trendline="ols")
    fig.update_traces(textposition='top center', textfont_size=6)
    fig.update_layout(shapes=shapes)
    fig.update_xaxes(title_text=f"<span style='font-size: 12px;'>Percent {demo} Residents (2010 U.S. Census)</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Mean Annual Number of 'Excess' Misconduct Complaints</span>")
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Per-Precinct Mean Annual 'Excess' Misconduct Complaints vs. Percent {demo} Residents ({start}-{stop})",
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.show()
    
    results = px.get_trendline_results(fig)
    return df, results.px_fit_results.iloc[0].summary()

# Function to generate Figures A3, A8, A9
def annual_subst_complaints_vs_prop_demo_reg(df, start, stop, figno, demo, ign_pcts=[]):
    df = df.copy()
    df = df[df[f"2010_Percent_{demo}_Residents"].notna()]
    df["Precinct"] = df["Precinct"].astype(int)
    df = df.rename(columns={f"2010_Percent_{demo}_Residents": f"2010 Percent {demo} Residents", "Annual_Mean_Substantiated": "Mean Annual Substantiated Misconduct Complaints"})
    df["Annual_Mean_Substantiated_Pred"] = b0s + b1s * df["Annual_Mean_Crime_Reports"]
    df["Mean Annual 'Excess' Substantiated Complaints"] = df["Mean Annual Substantiated Misconduct Complaints"] - df["Annual_Mean_Substantiated_Pred"]
    
    shapes = seaborn_conf_int(df, f"2010 Percent {demo} Residents", "Mean Annual 'Excess' Substantiated Complaints") 
    fig = px.scatter(df, x=df[f"2010 Percent {demo} Residents"], y=df["Mean Annual 'Excess' Substantiated Complaints"], color=df.Precinct, text=df.Precinct, trendline="ols")
    fig.update_traces(textposition='top center', textfont_size=6)
    fig.update_layout(shapes=shapes)
    fig.update_xaxes(title_text=f"<span style='font-size: 12px;'>Percent {demo} Residents (2010 U.S. Census)</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Mean Annual Number of 'Excess' Substantiated Misconduct Complaints</span>")
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Per-Precinct Mean Annual 'Excess' Substantiated Misconduct Complaints vs. Percent {demo} Residents ({start}-{stop})",
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.show()
    
    results = px.get_trendline_results(fig)
    return df, results.px_fit_results.iloc[0].summary()

# Function to generate Figure 6
def annual_complaints_vs_complaints_per_officer_reg(df, start, stop, figno, ign_pcts=[]):
    df = df.copy()
    df = df.rename(columns={f"Mean_Complaints_per_Officer": "Mean Complaints Per Accused Officer", "Annual_Mean_Complaints": "Mean Annual Misconduct Complaints"})
    df["Annual_Mean_Complaints_Pred"] = b0 + b1 * df["Annual_Mean_Crime_Reports"]
    df["Mean Annual 'Excess' Complaints"] = df["Mean Annual Misconduct Complaints"] - df["Annual_Mean_Complaints_Pred"]
    
    shapes = seaborn_conf_int(df, f"Mean Complaints Per Accused Officer", "Mean Annual 'Excess' Complaints") 
    fig = px.scatter(df, x=df[f"Mean Complaints Per Accused Officer"], y=df["Mean Annual 'Excess' Complaints"], text=df.Precinct, trendline="ols")
    fig.update_traces(textposition='top center', textfont_size=6)
    fig.update_layout(shapes=shapes)
    fig.update_xaxes(title_text=f"<span style='font-size: 12px;'>Mean Annual Number of Misconduct Complaints Per Accused Officer</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Mean Annual Number of 'Excess' Misconduct Complaints</span>")
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Per-Precinct Mean Annual 'Excess' Misconduct Complaints vs. Mean Misconduct Complaints Per Accused Officer ({start}-{stop})",
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.show()
    
    results = px.get_trendline_results(fig)
    return df, results.px_fit_results.iloc[0].summary()

# Function to generate Figure A4
def annual_subst_complaints_vs_complaints_per_officer_reg(df, start, stop, figno, ign_pcts=[]):
    df = df.copy()
    df = df.rename(columns={f"Mean_Substantiated_per_Officer": "Mean Substantiated Complaints Per Accused Officer", "Annual_Mean_Substantiated": "Mean Annual Substantiated Misconduct Complaints"})
    df["Annual_Mean_Substantiated_Pred"] = b0s + b1s * df["Annual_Mean_Crime_Reports"]
    df["Mean Annual 'Excess' Substantiated Complaints"] = df["Mean Annual Substantiated Misconduct Complaints"] - df["Annual_Mean_Substantiated_Pred"]
    
    shapes = seaborn_conf_int(df, f"Mean Substantiated Complaints Per Accused Officer", "Mean Annual 'Excess' Substantiated Complaints") 
    fig = px.scatter(df, x=df[f"Mean Substantiated Complaints Per Accused Officer"], y=df["Mean Annual 'Excess' Substantiated Complaints"], text=df.Precinct, trendline="ols")
    fig.update_traces(textposition='top center', textfont_size=6)
    fig.update_layout(shapes=shapes)
    fig.update_xaxes(title_text=f"<span style='font-size: 12px;'>Mean Annual Number of Substantiated Misconduct Complaints Per Accused Officer</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Mean Annual Number of 'Excess' Substantiated Misconduct Complaints</span>")
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Per-Precinct Mean Annual 'Excess' Substantiated Misconduct Complaints vs. Mean Substantiated Misconduct Complaints Per Accused Officer ({start}-{stop})",
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.show()
    
    results = px.get_trendline_results(fig)
    return df, results.px_fit_results.iloc[0].summary()

# Function to generate Figure 7
def annual_complaints_per_officer_vs_prop_demo_reg(df, start, stop, figno, demo, ign_pcts=[]):
    df = df.copy()
    df = df[df[f"2010_Percent_{demo}_Residents"].notna()]
    df["Precinct"] = df["Precinct"].astype(int)
    df = df.rename(columns={f"Mean_Complaints_per_Officer": "Mean Complaints Per Accused Officer", f"2010_Percent_{demo}_Residents": f"2010 Percent {demo} Residents"})
    shapes = seaborn_conf_int(df, f"2010 Percent {demo} Residents", "Mean Complaints Per Accused Officer") 
    fig = px.scatter(df, x=df[f"2010 Percent {demo} Residents"], y=df["Mean Complaints Per Accused Officer"], color=df.Precinct, text=df.Precinct, trendline="ols")
    fig.update_traces(textposition='top center', textfont_size=6)
    fig.update_layout(shapes=shapes)
    fig.update_xaxes(title_text=f"<span style='font-size: 12px;'>Percent {demo} Residents (2010 U.S. Census)</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Mean Annual Number of Misconduct Complaints Per Accused Officer</span>")
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Per-Precinct Mean Misconduct Complaints Per Accused Officer vs. Percent {demo} Residents ({start}-{stop})",
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.show()
    
    results = px.get_trendline_results(fig)
    return df, results.px_fit_results.iloc[0].summary()

# Function to generate Figure A5
def annual_subst_complaints_per_officer_vs_prop_demo_reg(df, start, stop, figno, demo, ign_pcts=[]):
    df = df.copy()
    df = df[df[f"2010_Percent_{demo}_Residents"].notna()]
    df["Precinct"] = df["Precinct"].astype(int)
    df = df.rename(columns={f"Mean_Substantiated_per_Officer": "Mean Substantiated Complaints Per Accused Officer", f"2010_Percent_{demo}_Residents": f"2010 Percent {demo} Residents"})
    shapes = seaborn_conf_int(df, f"2010 Percent {demo} Residents", "Mean Substantiated Complaints Per Accused Officer") 
    fig = px.scatter(df, x=df[f"2010 Percent {demo} Residents"], y=df["Mean Substantiated Complaints Per Accused Officer"], color=df.Precinct, text=df.Precinct, trendline="ols")
    fig.update_traces(textposition='top center', textfont_size=6)
    fig.update_layout(shapes=shapes)
    fig.update_xaxes(title_text=f"<span style='font-size: 12px;'>Percent {demo} Residents (2010 U.S. Census)</span>")
    fig.update_yaxes(title_text="<span style='font-size: 12px;'>Mean Annual Number of Substantiated Misconduct Complaints Per Accused Officer</span>")
    fig.update_layout(
        title={
            'text': f"<b>Figure {figno.capitalize()}</b>: Per-Precinct Mean Substantiated Misconduct Complaints Per Accused Officer vs. Percent {demo} Residents ({start}-{stop})",
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig.show()
    
    results = px.get_trendline_results(fig)
    return df, results.px_fit_results.iloc[0].summary()

# Generate visualizations
print("Generating Fig 1")
annual_complaints(ccrb, 1986, 2019, "1")
print("Generating Fig 2")
annual_complaints_officers_crimes(ccrb, 1986, 2019, "2")
print("Generating Fig 3")
annual_complaints_vs_officers_reg(ccrb, 1986, 2018, "3")
print("Generating Fig A1")
annual_subst_complaints_vs_officers_reg(ccrb, 1986, 2018, "a1")

print("Flattening precinct-year data")
flat = compile_precincts(ccrb)

print("Generating Fig 4")
annual_complaints_vs_reported_crime_reg(flat, 2006, 2019, "4")
print("Generating Fig A2")
annual_subst_complaints_vs_reported_crime_reg(flat, 2006, 2019, "a2")
print("Generating Fig 5")
annual_complaints_vs_prop_demo_reg(flat, 2006, 2019, "5", "Black")
print("Generating Fig A6")
annual_complaints_vs_prop_demo_reg(flat, 2006, 2019, "a6", "Non-Hispanic White")
print("Generating Fig A7")
annual_complaints_vs_prop_demo_reg(flat, 2006, 2019, "a7", "Non-Hispanic Asian")
print("Generating Figure A3")
annual_subst_complaints_vs_prop_demo_reg(flat, 2006, 2019, "a3", "Black")
print("Generating Figure A8")
annual_subst_complaints_vs_prop_demo_reg(flat, 2006, 2019, "a8", "Non-Hispanic White")
print("Generating Figure A9")
annual_subst_complaints_vs_prop_demo_reg(flat, 2006, 2019, "a9", "Non-Hispanic Asian")
print("Generating Figure 6")
annual_complaints_vs_complaints_per_officer_reg(flat, 2006, 2019, "6")
print("Generating Figure A4")
annual_subst_complaints_vs_complaints_per_officer_reg(flat, 2006, 2019, "a4")
print("Generating Figure 7")
annual_complaints_per_officer_vs_prop_demo_reg(flat, 2006, 2019, "7", "Black")
print("Generating Figure A5")
annual_subst_complaints_per_officer_vs_prop_demo_reg(flat, 2006, 2019, "a5", "Black")
