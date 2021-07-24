#!/usr/bin/env python
# coding: utf-8

"""Module to grab Google Scholar citation data and generate updated summaries on AWS S3.

Examples:
    $ python update.py

"""

__version__ = "0.1"

# imports
import boto3
import plotly.express as px
import os
from botocore.exceptions import ClientError
from scholarly import scholarly
from typing import Dict, Any


# Amazon S3 constants
BUCKET_NAME = os.environ.get("AWS_STORAGE_BUCKET_NAME")
PROFILE_ID = "_FLoVbkAAAAJ&hl"


def main(profile_id: str) -> None:
    """Wrapper to retrieve data, write updated csv, and use it to write updated graph

    Args:
        profile_id (str): google scholar author profile ID
    """
    get_data_process_and_write_files(profile_id)


def get_data_process_and_write_files(profile_id: str) -> None:
    """Wrapper to run all module functions.

    Args:
        profile_id (str): google scholar author profile ID
    """
    scholar_data = get_scholar_data(profile_id)
    fig = make_citation_figure(scholar_data)
    metrics = make_citation_metrics(scholar_data)

    save_to_s3("citations.png", fig)
    save_to_s3("citations.csv", metrics)


def get_scholar_data(profile_id: str) -> Dict[str, Any]:
    author = scholarly.search_author_id(profile_id)
    scholarly.fill(author, sections=["basics", "indices", "counts"])

    return author


def make_citation_figure(author: Dict[str, Any]) -> Any:
    if isinstance(author.get("cites_per_year"), dict):
        auth = author.get("cites_per_year")
    else:
        auth = {2021: 0}
    fig = px.bar(x=auth.keys(), y=auth.values())
    fig.update_xaxes(title="Year", dtick=1)
    fig.update_yaxes(title="Citations per Year")

    return fig


def make_citation_metrics(author: Dict[str, Any]) -> None:
    pass


def save_to_s3(object_name: str, contents: str) -> None:
    """Upload citation files to an S3 bucket.

    Args:
        object_name (str): name of object to store
        contents: string fileobject of contents

    Returns:
        True if file was uploaded, else False
    """

    s3_client = boto3.client("s3")
    # Citation Figure
    try:
        _ = s3_client.put_object(
            Bucket=BUCKET_NAME, Key=object_name, Body=contents.getvalue()
        )
    except ClientError as e:
        print(e)
        return False
    return True


if __name__ == "__main__":
    main(PROFILE_ID)
