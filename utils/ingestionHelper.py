import requests
import os
import shutil
import zipfile
from pyspark.sql import SparkSession


def download_unzip_and_save_as_table(url, tmp_base_path, table_name, file_format):
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
