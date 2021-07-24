#!/usr/bin/env python
# coding: utf-8

"""Module to grab Google Scholar citation data and generate updated summaries on AWS S3.

Examples:
    $ python update.py

"""

__version__ = "0.1"

# imports
from datetime import date
import boto3
import os
import io
from botocore.exceptions import ClientError


# Amazon S3 constants
BUCKET_NAME = os.environ.get("AWS_STORAGE_BUCKET_NAME")
OBJECT_NAME = "scholar.csv"


def main(url, filename):
    """Wrapper to retrieve data, write updated csv, and use it to write updated graph

    Args:
        url (str): url of Tableau dashboard to parse
        filename (str): output csv filename
    """
    get_data_process_and_write_csv(url, filename)


def get_data_process_and_write_csv(url, filename):
    """Wrapper to run all module functions.

    Args:
        url (str): url of page with embedded Tableau display
        filename (str): output .csv file to which we will append data
    """
    url_data = get_tableau_url(url)
    data_url, data_dict = parse_tableau_config_return_data_url(url_data)
    values = parse_tableau_data(data_url, data_dict)
    values_with_date = prepend_date(values)
    current_data = read_current_s3()
    final_data = append_new_data(current_data, values_with_date)
    save_to_s3(final_data)


def read_current_s3():
    """Retrieve latest covid data from Amazon s3 bucket and process as a csv fileobject.

    Returns:
        io.String: string object of covid data
    """
    s3_client = boto3.client("s3")
    bytes_buffer = io.BytesIO()

    s3_client.download_fileobj(
        Bucket=BUCKET_NAME, Key=OBJECT_NAME, Fileobj=bytes_buffer
    )
    byte_value = bytes_buffer.getvalue()
    str_value = byte_value.decode()
    return io.StringIO(str_value)


def save_to_s3(final_data):
    """Upload a file to an S3 bucket.

    Args:
        final_data: string fileobject of final data

    Returns:
        True if file was uploaded, else False
    """

    s3_client = boto3.client("s3")
    try:
        _ = s3_client.put_object(
            Bucket=BUCKET_NAME, Key=OBJECT_NAME, Body=final_data.getvalue()
        )
    except ClientError as e:
        print(e)
        return False
    return True


def append_new_data(current_data, values):
    """Write Tableau data values to a simple csv fileobject.

    Args:
        current_data: output fileobject
        values (List[str]): list of Tableau data values
    """
    string_to_write = ",".join(str(v) for v in values) + "\n"
    current_data.seek(0, 2)
    current_data.write(string_to_write)
    current_data.seek(0)
    return current_data


if __name__ == "__main__":
    main(URL, FILENAME)
