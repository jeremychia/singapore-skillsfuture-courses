import argparse
import os
from datetime import datetime

import pandas as pd
import pandas_gbq
from courses.api_client import fetch_course_data
from courses.config import BIGQUERY_TABLES, PRIMARY_KEYS, PROJECT_ID, QUERY_ROWS
from courses.data_processing import parse_response_to_dataframes


def main(start_row_arg=0):
    """
    Fetches course data from an API, processes it, and uploads it to BigQuery.

    Args:
        start_row_arg (int): The starting row number for data retrieval.
    """
    all_dataframes = {table: pd.DataFrame() for table in BIGQUERY_TABLES}

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tokens/gcp_token.json"

    i = start_row_arg // QUERY_ROWS  # Calculate initial 'i' based on start_row_arg

    while True:
        start_row = i * QUERY_ROWS
        response_json = fetch_course_data(start=start_row)

        if not response_json or "grouped" not in response_json:
            print(f"API call failed or returned unexpected data: {response_json}")
            break

        course_docs_list = response_json["grouped"]["GroupID"]["groups"]
        print(f"Adding rows: {len(course_docs_list)}, start row: {start_row}")

        if not course_docs_list:
            break

        new_dataframes = parse_response_to_dataframes(course_docs_list)
        for df, table in zip(new_dataframes, BIGQUERY_TABLES):
            all_dataframes[table] = pd.concat(
                [all_dataframes[table], df], ignore_index=True
            )
        i += 1

    accessed_at_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for df in all_dataframes.values():
        df["_accessed_at"] = accessed_at_timestamp

    # Convert specific columns to string before upload
    if "courses" in all_dataframes:
        all_dataframes["courses"]["quality_count_respondents"] = all_dataframes[
            "courses"
        ]["quality_count_respondents"].astype(str)
        all_dataframes["courses"]["quality_rating_out_of_5"] = all_dataframes[
            "courses"
        ]["quality_rating_out_of_5"].astype(str)

    # Upload DataFrames to BigQuery
    for table_name, df in all_dataframes.items():
        pandas_gbq.to_gbq(
            dataframe=df,
            destination_table=BIGQUERY_TABLES[table_name],
            project_id=PROJECT_ID,
            if_exists="append",
        )

    # Deduplicate the tables
    for table_name in BIGQUERY_TABLES:
        try:
            df = pandas_gbq.read_gbq(BIGQUERY_TABLES[table_name], project_id=PROJECT_ID)

            before_dedup_rows = len(df)

            df = df.sort_values(by="_accessed_at", ascending=False)
            df_deduped = df.drop_duplicates(
                subset=PRIMARY_KEYS[table_name], keep="first"
            )

            after_dedup_rows = len(df_deduped)

            pandas_gbq.to_gbq(
                dataframe=df_deduped,
                destination_table=BIGQUERY_TABLES[table_name],
                project_id=PROJECT_ID,
                if_exists="replace",
            )

            print(
                f"Table {BIGQUERY_TABLES[table_name]}: Before {before_dedup_rows} rows, After {after_dedup_rows} rows. Deduplication complete."
            )
        except Exception as e:
            print(f"Error processing table {BIGQUERY_TABLES[table_name]}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch course data and upload to BigQuery."
    )
    parser.add_argument(
        "--start_row",
        type=int,
        default=0,
        help="Starting row number for data retrieval.",
    )
    args = parser.parse_args()

    main(args.start_row)
