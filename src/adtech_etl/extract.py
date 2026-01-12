import pandas as pd
from pathlib import Path

DATA_PATH = Path("data")

def extract_excel(file_path: str) -> pd.DataFrame:
    """
    Read all sheets from Excel and union into one DataFrame
    """
    xls = pd.ExcelFile(file_path)

    dfs = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        df["source_sheet"] = sheet  # lineage
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


if __name__ == "__main__":
    df = extract_excel("../../data/ad_events.csv")
    print(df.head())
