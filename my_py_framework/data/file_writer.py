def write_to_excel(dfs: dict, filepath):
    with pd.ExcelWriter(filepath) as writer:
        for sheet_name, df in dfs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

def write_to_csv(df, filepath):
    df.to_csv(filepath, index=False)
