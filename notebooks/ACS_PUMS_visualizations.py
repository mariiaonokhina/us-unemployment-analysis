import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

df = pd.read_parquet("../data/processed/parquet_pums/pums_all_cleaned.parquet")

df_esr = (
    df.groupby("ESR")
      .size()
      .reset_index(name="Count")
      .sort_values("ESR")
)

state_map = {
    1:"Alabama", 2:"Alaska", 4:"Arizona", 5:"Arkansas", 6:"California",
    8:"Colorado", 9:"Connecticut", 10:"Delaware", 11:"District of Columbia",
    12:"Florida", 13:"Georgia", 15:"Hawaii", 16:"Idaho", 17:"Illinois",
    18:"Indiana", 19:"Iowa", 20:"Kansas", 21:"Kentucky", 22:"Louisiana",
    23:"Maine", 24:"Maryland", 25:"Massachusetts", 26:"Michigan",
    27:"Minnesota", 28:"Mississippi", 29:"Missouri", 30:"Montana",
    31:"Nebraska", 32:"Nevada", 33:"New Hampshire", 34:"New Jersey",
    35:"New Mexico", 36:"New York", 37:"North Carolina", 38:"North Dakota",
    39:"Ohio", 40:"Oklahoma", 41:"Oregon", 42:"Pennsylvania", 44:"Rhode Island",
    45:"South Carolina", 46:"South Dakota", 47:"Tennessee", 48:"Texas",
    49:"Utah", 50:"Vermont", 51:"Virginia", 53:"Washington",
    54:"West Virginia", 55:"Wisconsin", 56:"Wyoming"
}

df["STATE_NAME"] = df["ST"].map(state_map)

ESR_MAP = {
    0: "N/A",
    1: "Civilian: At Work",
    2: "Civilian: Has Job, Not Working",
    3: "Unemployed",
    4: "Armed Forces: At Work",
    5: "Armed Forces: Not at Work",
    6: "Not in Labor Force"
}

df["ESR_Label"] = df["ESR"].map(ESR_MAP)

grouped = (
    df.groupby(["STATE_NAME", "ESR_Label"])
      .size()
      .reset_index(name="Count")
)

fig = px.bar(
    grouped,
    x="STATE_NAME",
    y="Count",
    color="ESR_Label",
    title="Employment Status Counts by State (with ESR Descriptions)",
    labels={"STATE_NAME": "State", "Count": "Population", "ESR_Label": "Employment Status"},
)

fig.update_layout(
    width=1000,
    height=800,
    xaxis_tickangle=45,
    legend_title="Employment Status"
)

fig.show()

fig = px.histogram(
    df,
    x="AGEP",
    nbins=50,
    marginal="box",
    color="ESR_Label",
    title="Age Distribution of Respondents (Colored by Employment Status)",
    labels={
        "AGEP": "Age",
        "ESR_Label": "Employment Status"
    }
)

fig.show()

