from datetime import datetime

import pandas_gbq
from course_details.config import PROJECT_ID


def get_course_reference_numbers():
    sql = """
    SELECT DISTINCT course_reference_number
    FROM `jeremy-chia.sg_skillsfuture.courses`
    ORDER BY course_reference_number
    """
    return list(
        pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)["course_reference_number"]
    )


def upload_to_gbq(dataframe, table_name):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dataframe["_accessed_at"] = timestamp
    pandas_gbq.to_gbq(
        dataframe=dataframe,
        destination_table=table_name,
        project_id=PROJECT_ID,
        if_exists="append",
    )
