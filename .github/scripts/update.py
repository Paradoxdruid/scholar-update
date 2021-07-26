#!/usr/bin/env python
# coding: utf-8

"""Module to grab Google Scholar citation data and generate updated summaries on AWS S3.

Examples:
    $ python update.py

"""

__version__ = "0.1"

# imports
import boto3
import json
import plotly.express as px
import os
from botocore.exceptions import ClientError
from io import BytesIO
from scholarly import scholarly
from typing import Dict, Any


# Amazon S3 constants
BUCKET_NAME: str = os.environ.get("AWS_STORAGE_BUCKET_NAME")
PROFILE_ID: str = os.environ.get("SCHOLAR_PROFILE_ID")


def main(profile_id: str, bucket_name: str) -> None:
    """Wrapper to retrieve data, write updated csv, and use it to write updated graph

    Args:
        profile_id (str): google scholar author profile ID
        bucket_name (str): AWS bucket name
    """
    get_data_process_and_write_files(profile_id, bucket_name)


def get_data_process_and_write_files(profile_id: str, bucket_name: str) -> None:
    """Wrapper to run all module functions.

    Args:
        profile_id (str): google scholar author profile ID
        bucket_name (str): AWS bucket name
    """
    scholar_data: Dict[str, Any] = get_scholar_data(profile_id)
    fig: BytesIO = make_citation_figure(scholar_data)
    metrics: BytesIO = make_citation_metrics(scholar_data)

    save_to_s3("citations.png", bucket_name, fig)
    save_to_s3("citations.json", bucket_name, metrics)


def get_scholar_data(profile_id: str) -> Dict[str, Any]:
    author: Dict[str, Any] = scholarly.search_author_id(profile_id)
    scholarly.fill(author, sections=["basics", "indices", "counts"])

    return author


def make_citation_figure(author: Dict[str, Any]) -> BytesIO:
    if isinstance(author.get("cites_per_year"), dict):
        auth: Dict[int, int] = author.get("cites_per_year")
    else:
        auth = {2021: 0}
    fig: Any = px.bar(
        x=auth.keys(), y=auth.values(), template="seaborn", width=400, height=250,
    )
    fig.update_xaxes(title="", dtick=1, tickangle=-90)
    fig.update_yaxes(title="Citations", title_standoff=2, dtick=25)

    wrapper: BytesIO = BytesIO()
    fig.write_image(wrapper, format="png")
    wrapper.seek(0)
    return wrapper


def make_citation_metrics(author: Dict[str, Any]) -> BytesIO:
    citations_dict: Dict[str, int] = {
        "h-index": author["hindex"],
        "citations": author["citedby"],
        "i10-index": author["i10index"],
    }
    wrapper: BytesIO = BytesIO()
    wrapper.write(json.dumps(citations_dict).encode())
    wrapper.seek(0)

    return wrapper


def save_to_s3(object_name: str, bucket_name: str, contents: BytesIO) -> None:
    """Upload citation files to an S3 bucket.

    Args:
        object_name (str): name of object to store
        bucket_name (str): bucket name
        contents (BytesIO): bytes fileobject of contents

    Returns:
        True if file was uploaded, else False
    """

    s3_client: Any = boto3.client("s3")
    # Citation Figure
    try:
        _ = s3_client.upload_fileobj(
            contents, bucket_name, object_name, ExtraArgs={"ACL": "public-read"}
        )
    except ClientError as e:
        print(e)
        return False
    return True


if __name__ == "__main__":
    main(PROFILE_ID, BUCKET_NAME)
