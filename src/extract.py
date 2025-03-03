import requests

from typing import Dict
from pandas import DataFrame, read_csv, read_json, to_datetime

COUNTRY = "BR"

def temp() -> DataFrame:
    """Get the temperature data.
    Returns:
        DataFrame: A dataframe with the temperature data.
    """
    return read_csv("data/temperature.csv")

def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Get the public holidays for the given year for Brazil.
    Args:
        public_holidays_url (str): url to the public holidays.
        year (str): The year to get the public holidays for.
    Raises:
        SystemExit: If the request fails.
    Returns:
        DataFrame: A dataframe with the public holidays.
    """
    fullURL = f"{public_holidays_url}/{year}/{COUNTRY}"
    response = requests.get(fullURL)
    try :
        response.raise_for_status()
        data = response.json()
        df = DataFrame(data)
        df.drop(columns=["types", "counties"], inplace=True)
        df["date"] = to_datetime(df["date"], format="%Y-%m-%d")
        return df
    except requests.exceptions.RequestException as e:
        raise SystemExit(f"Error {response.status_code}: No se pudo obtener los datos. {e}")
    except ValueError:
        raise SystemExit(f"La respuesta no es un JSON valido")
        


def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes.
    Args:
        csv_folder (str): The path to the csv's folder.
        csv_table_mapping (Dict[str, str]): The mapping of the csv file names to the
        table names.
        public_holidays_url (str): The url to the public holidays.
    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the table names and values as
        the dataframes.
    """
    dataframes = {
        table_name: read_csv(f"{csv_folder}/{csv_file}")
        for csv_file, table_name in csv_table_mapping.items()
    }

    holidays = get_public_holidays(public_holidays_url, "2017")

    dataframes["public_holidays"] = holidays

    return dataframes
