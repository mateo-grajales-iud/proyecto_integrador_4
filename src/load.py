from typing import Dict

from pandas import DataFrame
from sqlalchemy.engine.base import Engine


def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Load the dataframes into the sqlite database.

    Args:
        data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
        and values as the dataframes.
    """
    for key in data_frames:
        df = data_frames[key]
        df.to_sql(name=key, con=database, if_exists='replace', index=False)