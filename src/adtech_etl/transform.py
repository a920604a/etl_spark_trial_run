import pandas as pd

VALID_EVENT_TYPES = {"impression", "click", "conversion"}

def clean_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic data cleaning rules
    """
    df = df.copy()

    # parse datetime
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

    # basic validation
    df = df.dropna(subset=["event_time", "user_id", "ad_id", "campaign_id"])
    df = df[df["event_type"].isin(VALID_EVENT_TYPES)]
    df = df[df["cost"] >= 0]

    # derived columns
    df["event_date"] = df["event_time"].dt.date

    return df
