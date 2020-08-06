import pandas as pd
from functools import reduce


renames = {
    "advanced_keeping_new": {
        "Launch%": "PctLaunchedPasses",
        "Launch%.1": "PctLaunchedGK",
        "#OPA/90": "SweeperActions",
        "Stp%": "PctCrossesDefended",
        "/90": "PSxG+/90",
        "Cmp": "LaunchPassCmp",
        "Att": "LaunchPassAtt",
        "Att.2": "GoalKickAtt",
        "Cmp%": "LaunchCmpPct",
        "Att.1": "PassesAttempted",
        "Thr": "PassesThrown",
        "AvgLen": "PassAvgLen",
        "AvgLen.1": "GKAvgLen",
        "Opp": "OpponentCrossesAtt",
        "Stp": "CrossesStopped",
    },
    "advanced_keeping": {"/90": "PSxG+/90_old", "PSxG": "PSxG_old", "PSxG/SoT": "PSxG/SoT_old", "PSxG+/-": "PSxG+/-_old",},
    "passing": {
        "Cmp": "TotalCmp",
        "Att": "TotalAtt",
        "Cmp%": "TotalCmpPct",
        "Cmp.1": "ShortCmp",
        "Att.1": "ShortAtt",
        "Cmp%.1": "ShortCmpPct",
        "Cmp.2": "MedCmp",
        "Att.2": "MedAtt",
        "Cmp%.2": "MedCmpPct",
        "Cmp.3": "LongCmp",
        "Att.3": "LongAtt",
        "Cmp%.3": "LongCmpPct",
    },
    "pass_types": {"Cmp": "PassCmp", "Out.1": "PassOut"},
}


def merge_data_sets(datasets, years, min_90s):
    def _col_join(a, b):
        j = "_".join([a, b])
        if j[0] == "_":
            return j[1:]
        else:
            return j

    def _join_columns(df):
        df.columns = [
            _col_join(a, b)
            for a, b in zip([c if c[:7] != "Unnamed" else "" for c in df.columns.get_level_values(0)], df.columns.get_level_values(1),)
        ]

    data_sets_dfs = {}
    for data_set in datasets:
        data_sets_dfs[data_set] = {}
        for year in years:
            data_sets_dfs[data_set][year] = pd.read_csv(f"input_data/{data_set}_{year}.csv", header=[0, 1])
            _join_columns(data_sets_dfs[data_set][year])

            data_sets_dfs[data_set][year]["year"] = year

        data_sets_dfs[data_set] = pd.concat(data_sets_dfs[data_set].values(), sort=False)
        data_sets_dfs[data_set] = data_sets_dfs[data_set].rename(columns={s: s.replace(" ", "_").lower() for s in data_sets_dfs[data_set].columns})
        data_sets_dfs[data_set]["player"] = data_sets_dfs[data_set]["player"].apply(lambda x: x.split("\\")[0])
        data_sets_dfs[data_set]["player_year"] = data_sets_dfs[data_set]["player"] + data_sets_dfs[data_set]["year"]
        # data_sets_dfs[data_set] = data_sets_dfs[data_set].set_index("player_year")

    def _merge_df(df1, df2):
        df = pd.merge(left=df1, right=df2, left_on=["player_year", "squad"], right_on=["player_year", "squad"], suffixes=("", "_discard"),)
        df = df[[c for c in df.columns if "_discard" not in c]]
        return df

    player_df = reduce(lambda df1, df2: _merge_df(df1, df2), data_sets_dfs.values())
    player_df["competition"] = player_df["comp"].apply(lambda x: x.split(" ")[0].upper())
    return player_df.loc[player_df["90s"] >= min_90s].reset_index()


if __name__ == "__main__":
    df = merge_data_sets(["passing", "shooting", "standard", "pass_type", "goal_creation", "defense"], ["2020"], 0)
    df.to_csv("output_data/player_data.csv")
