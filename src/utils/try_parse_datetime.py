import pandas as pd
from dateutil.parser import parse

def try_parse_datetime(series, n=10):
    """Попробовать распознать колонку как datetime, проверяя первые n значений"""
    try:
        sample = series.head(n, compute=True)
        for val in sample:
            if pd.isnull(val):
                continue
            parse(val)  # выбросит исключение, если не похоже на дату
        return True
    except Exception:
        return False
