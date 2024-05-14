import requests
import os
import shutil
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def download_unzip_and_save_as_table(url, tmp_base_path, table_name, file_format, game_by_game=False):
    """
    Downloads a ZIP file from a URL, checks if it is a ZIP file, unzips it, and saves the contents as a table in DBFS storage.

    :param url: URL of the ZIP file to download.
    :param dbfs_table_path: Path in DBFS where the table should be stored.
    :param table_name: Name of the table to be created.
    """

    temp_path = tmp_base_path + table_name + "/" + table_name + file_format

    # Check if it is a ZIP file and unzip
    if file_format == ".zip":
        # Download the file
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(temp_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)
        print(f"File downloaded and saved to {temp_path}")

        with zipfile.ZipFile(temp_path, "r") as zip_ref:
            # Extract all the contents into the DBFS table path
            zip_ref.extractall(tmp_base_path + table_name + "/")
            print(
                f"The file downloaded from {url} was a ZIP file and successfully copied to {tmp_base_path}."
            )
            temp_path = tmp_base_path + table_name + "/" + table_name + ".csv"
    else:
        # Download the file
        if game_by_game:
            temp_path = tmp_base_path + "player_game_stats/" + table_name + file_format
            
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            # Open the file in write mode with UTF-8 encoding
            with open(temp_path, "w", encoding="utf-8") as f:
                # Iterate over the response in text mode and write to the file
                for chunk in r.iter_lines(decode_unicode=True):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk + '\n')

        print(
            f"The file downloaded from {url} is not a ZIP file and successfully copied to {temp_path}."
        )            

    return temp_path



def select_rename_columns(
    df: DataFrame, select_cols: list, col_abrev: str, situation: str, season: int = 2023
) -> DataFrame:
    """
    Selects and renames columns of a DataFrame based on input criteria.

    Args:
        df (DataFrame): The input DataFrame.
        select_cols (list): A list of column names to select.
        col_abrev (str): An abbreviation to add as a prefix to the column names.
        situation (str): The situation criteria for filtering the DataFrame.
        season (int, optional): The season for filtering the DataFrame (default: 2023).

    Returns:
        DataFrame: The modified DataFrame with selected and renamed columns.
    """

    df_filtered = (
        df.filter((col("season") == 2023) & (col("situation") == situation))
        .select(select_cols)
        .withColumn("gameDate", col("gameDate").cast("string"))
        .withColumn("gameDate", regexp_replace("gameDate", "\\.0$", ""))
        .withColumn("gameDate", to_date(col("gameDate"), "yyyyMMdd"))
        .withColumnRenamed("name", "shooterName")
    )
    player_stat_columns = df_filtered.columns
    for column in player_stat_columns:
        if column not in [
            "playerId",
            "season",
            "shooterName",
            "gameId",
            "playerTeam",
            "opposingTeam",
            "home_or_away",
            "gameDate",
            "position",
        ]:
            if "I_F_" in column:
                new_column = column.replace("I_F_", "")
                df_filtered = df_filtered.withColumnRenamed(
                    column, f"{col_abrev}{new_column}"
                )
            else:
                df_filtered = df_filtered.withColumnRenamed(
                    column, f"{col_abrev}{column}"
                )

    return df_filtered


def select_rename_game_columns(
    df: DataFrame, select_cols: list, col_abrev: str, situation: str, season: int = 2023
) -> DataFrame:
    """
    Selects and renames columns of a DataFrame (excluding 'name' and 'position' columns) based on input criteria.

    Args:
        df (DataFrame): The input DataFrame.
        select_cols (list): A list of column names to select.
        col_abrev (str): An abbreviation to add as a prefix to the column names.
        situation (str): The situation criteria for filtering the DataFrame.
        season (int, optional): The season for filtering the DataFrame (default: 2023).

    Returns:
        DataFrame: The modified DataFrame with selected and renamed columns.
    """

    df_filtered = (
        df.filter((col("season") == 2023) & (col("situation") == situation))
        .select(select_cols)
        .drop("name", "position")
        .withColumn("gameDate", col("gameDate").cast("string"))
        .withColumn("gameDate", regexp_replace("gameDate", "\\.0$", ""))
        .withColumn("gameDate", to_date(col("gameDate"), "yyyyMMdd"))
    )
    game_stat_columns = df_filtered.columns
    for column in game_stat_columns:
        if column not in [
            "situation",
            "season",
            "team",
            "name",
            "playerTeam",
            "home_or_away",
            "gameDate",
            "position",
            "opposingTeam",
            "gameId",
        ]:
            df_filtered = df_filtered.withColumnRenamed(column, f"{col_abrev}{column}")
            
    return df_filtered
