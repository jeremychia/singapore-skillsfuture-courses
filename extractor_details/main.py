import os
import time

import pandas as pd
from course_details.api_utils import fetch_course_details
from course_details.config import CHUNK_SIZE
from course_details.data_parsing import (
    parse_course_details,
    parse_course_runs,
    parse_job_roles,
    parse_mode_of_trainings,
    parse_trainers,
)
from course_details.database_utils import get_course_reference_numbers, upload_to_gbq


def chunk_list(input_list, chunk_size=1000):
    return [
        input_list[i : i + chunk_size] for i in range(0, len(input_list), chunk_size)
    ]


def get_all_courses_data(course_reference_numbers):
    all_courses = []
    all_trainers = []
    all_job_roles = []
    all_mode_of_trainings = []
    all_course_runs = []

    total_courses = len(course_reference_numbers)
    start_time = time.time()

    for idx, course_reference in enumerate(course_reference_numbers):
        start_course_time = time.time()
        response = fetch_course_details(course_reference)
        if response and response.status_code == 200 and "data" in response.json():
            course_detail_dict = response.json().get("data", {})

            course_details = parse_course_details(course_detail_dict)
            all_courses.append(course_details)

            all_mode_of_trainings.extend(parse_mode_of_trainings(course_detail_dict))
            all_course_runs.extend(parse_course_runs(course_detail_dict))
            all_trainers.extend(parse_trainers(course_detail_dict))
            all_job_roles.extend(parse_job_roles(course_detail_dict))

        course_time_taken = time.time() - start_course_time
        courses_processed = idx + 1
        courses_remaining = total_courses - courses_processed

        if courses_processed > 1:
            average_time_per_course = (time.time() - start_time) / courses_processed
            estimated_time_remaining = average_time_per_course * courses_remaining
            estimated_completion_time = start_time + (
                average_time_per_course * total_courses
            )
            estimated_time_remaining_minutes = estimated_time_remaining / 60
            estimated_completion_time_readable = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(estimated_completion_time)
            )
            print(
                f"Estimated time remaining: {estimated_time_remaining_minutes:.2f} minutes, complete at {estimated_completion_time_readable}"
            )
        else:
            print("Estimating remaining time...")

    course_details_df = pd.DataFrame([course.__dict__ for course in all_courses])
    trainers_df = pd.DataFrame(all_trainers)
    job_roles_df = pd.DataFrame([job_role.__dict__ for job_role in all_job_roles])
    mode_of_trainings_df = pd.DataFrame(
        [mode.__dict__ for mode in all_mode_of_trainings]
    )
    course_runs_df = pd.DataFrame(all_course_runs)

    return (
        course_details_df,
        trainers_df,
        job_roles_df,
        mode_of_trainings_df,
        course_runs_df,
    )


if __name__ == "__main__":
    # Set Google Cloud credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tokens/gcp_token.json"

    course_reference_numbers = get_course_reference_numbers()
    course_reference_numbers_list = chunk_list(course_reference_numbers, CHUNK_SIZE)

    for course_references in course_reference_numbers_list:
        (
            course_details_df,
            trainers_df,
            job_roles_df,
            mode_of_trainings_df,
            course_runs_df,
        ) = get_all_courses_data(course_references)

        upload_to_gbq(course_details_df, "sg_skillsfuture.course_details")
        upload_to_gbq(trainers_df, "sg_skillsfuture.trainers")
        upload_to_gbq(job_roles_df, "sg_skillsfuture.job_roles")
        upload_to_gbq(mode_of_trainings_df, "sg_skillsfuture.mode_of_trainings")
        upload_to_gbq(course_runs_df, "sg_skillsfuture.course_runs")
