import json

import pandas as pd


def census(df):
    columns_map = json.load(open("census-col-lookup.json", "r"))
    df = df.rename(columns=columns_map)
    df = df.drop([x for x in df.columns if x.startswith("P00")], axis=1)
    records = df.to_dict(orient="records")

    black_columns = [c for c in df.columns if c.startswith("R_") and "B" in c]
    nh_black_columns = [c for c in df.columns if c.startswith("NH_") and "B" in c]
    nh_asian_columns = [c for c in df.columns if c.startswith("NH_") and "A" in c and "B" not in c]

    for r in records:
        r["Black"] = sum([r[c] for c in black_columns])
        r["NH_Black"] = sum([r[c] for c in nh_black_columns])  # intermediate
        r["H_Black"] = r["Black"] - r["NH_Black"]  # intermediate
        r["Hispanics"] = r["Hispanics"] - r["H_Black"]
        r["NH_Asian"] = sum([r[c] for c in nh_asian_columns])
        r["NH_White"] = r["NH_W"]
        r["Others"] = r["Total_Population"] - r["Black"] - r["Hispanics"] - r["NH_Asian"] - r["NH_White"]

    new_df = pd.DataFrame.from_dict(records)
    new_df = new_df.dropna(subset=["precinct_2020"])
    precincts_group = new_df.groupby("precinct_2020")
    demo = precincts_group[["Total_Population", "Black", "Hispanics", "NH_Asian", "NH_White", "Others"]].sum().reset_index()
    demo["Black_Percentage"] = 100*demo["Black"]/demo['Total_Population']
    demo["Hispanics_Percentage"] = 100*demo["Hispanics"]/demo['Total_Population']
    demo["NH_Asian_Percentage"] = 100*demo["NH_Asian"]/demo['Total_Population']
    demo["NH_White_Percentage"] = 100*demo["NH_White"]/demo['Total_Population']
    demo["Others_Percentage"] = 100*demo["Others"]/demo['Total_Population']

    # demo.to_csv("precincts_demos.csv", index=False)
    return demo


