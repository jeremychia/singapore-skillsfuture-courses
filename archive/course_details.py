import os

import pandas_gbq

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"
PROJECT_ID = "jeremy-chia"

sql = """
select distinct course_reference_number
from `jeremy-chia.sg_skillsfuture.courses`
order by course_reference_number
"""

course_reference_numbers = list(
    pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)["course_reference_number"]
)


def chunk_list(input_list, chunk_size=1000):
    return [
        input_list[i : i + chunk_size] for i in range(0, len(input_list), chunk_size)
    ]


course_reference_numbers_list = chunk_list(course_reference_numbers)

import requests


def fetch_course_details(course_reference_number):
    url = "https://www.myskillsfuture.gov.sg/services/tex/individual/course-detail"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

    params = {
        "action": "get-course-by-ref-number",
        "refNumber": course_reference_number,
    }

    # Dynamically generate the Referer header based on the course reference number
    headers[
        "Referer"
    ] = f"https://www.myskillsfuture.gov.sg/content/portal/en/training-exchange/course-directory/course-detail.html?courseReferenceNumber={course_reference_number}"

    # Send GET request to the API
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        print(f"Data for course {course_reference_number} downloaded")
        return response
    else:
        print(
            f"Failed to retrieve data for course {course_reference_number}: {response.status_code}"
        )
        return None


from dataclasses import dataclass

import pandas as pd


# Define the data classes
@dataclass
class ModeOfTraining:
    course_reference_number: str
    mode_of_training_description: str


@dataclass
class CourseRun:
    course_reference_number: str
    course_run_id: str
    course_run_start_date: str
    course_run_end_date: str
    registration_start_date: str
    registration_end_date: str
    course_run_training_mode: str
    course_intake_size: str
    address_block: str
    address_street: str
    address_floor: str
    address_unit: str
    address_building: str
    address_postal_code: str
    address_room: str


@dataclass
class Trainer:
    course_reference_number: str
    course_run_id: str
    trainer_id: str
    trainer_id_number: str
    trainer_id_type_code: str
    trainer_uuid: str
    trainer_name: str
    trainer_email: str
    trainer_practice_area: str
    trainer_qualification_level: str
    trainer_experience: str


@dataclass
class JobRoleCourseDetails:
    course_reference_number: str
    job_role: str


@dataclass
class CourseDetails:
    course_reference_number: str
    course_title: str
    course_objective: str
    course_content: str
    entry_requirement: str
    training_days: str
    training_duration_hours: str
    count_attendees: str
    qualification_attained_id: str
    qualification_attained_name: str


# Parsing functions
def parse_mode_of_trainings(course_detail_dict):
    """Parse and return list of ModeOfTraining descriptions, handling None values safely."""
    if not isinstance(course_detail_dict, dict):
        course_detail_dict = {}

    course_reference_number = course_detail_dict.get("courseReferenceNumber", "")
    mode_of_trainings = course_detail_dict.get("modeOfTrainings", [])

    if not isinstance(mode_of_trainings, list):
        mode_of_trainings = []

    return [
        ModeOfTraining(
            course_reference_number=course_reference_number,
            mode_of_training_description=training_mode.get("description", "")
            if isinstance(training_mode, dict)
            else "",
        )
        for training_mode in mode_of_trainings
    ]


def parse_course_runs(course_detail_dict):
    """Parse and return list of CourseRun dictionaries, handling None values safely."""
    if not isinstance(course_detail_dict, dict):
        course_detail_dict = {}

    course_reference_number = course_detail_dict.get("courseReferenceNumber", "")
    course_run_list = course_detail_dict.get("courseRuns", [])

    if not isinstance(course_run_list, list):
        course_run_list = []

    course_runs = []
    for course_run in course_run_list:
        if not isinstance(course_run, dict):
            continue

        course_run_instance = {
            "course_reference_number": course_reference_number,
            "course_run_id": course_run.get("courseRunId", ""),
            "course_run_start_date": course_run.get("courseStartDate", ""),
            "course_run_end_date": course_run.get("courseEndDate", ""),
            "registration_start_date": course_run.get("registrationOpeningDate", ""),
            "registration_end_date": course_run.get("registrationClosingDate", ""),
            "course_run_training_mode": course_run.get("modeOfTraining", ""),
            "course_intake_size": course_run.get("intakeSize", ""),
            "address_block": course_run.get("block", ""),
            "address_street": course_run.get("street", ""),
            "address_floor": course_run.get("floor", ""),
            "address_unit": course_run.get("unit", ""),
            "address_building": course_run.get("building", ""),
            "address_postal_code": course_run.get("postalCode", ""),
            "address_room": course_run.get("room", ""),
        }
        course_runs.append(course_run_instance)

    return course_runs


def parse_trainers(course_detail_dict):
    """Parse and return list of Trainer dictionaries, handling None values safely."""
    if not isinstance(course_detail_dict, dict):
        return []

    course_reference_number = course_detail_dict.get("courseReferenceNumber", "")
    trainer_list = []

    course_runs = course_detail_dict.get("courseRuns", [])
    if not isinstance(course_runs, list):
        return trainer_list

    for course_run in course_runs:
        if not isinstance(course_run, dict):
            continue

        link_course_run_trainer = course_run.get("linkCourseRunTrainer", [])
        if not isinstance(link_course_run_trainer, list):
            continue

        for trainer in link_course_run_trainer:
            if not isinstance(trainer, dict):
                continue

            trainer_dict = trainer.get("trainer", {})
            if not isinstance(trainer_dict, dict):
                trainer_dict = {}

            trainer_instance = {
                "course_reference_number": course_reference_number,
                "course_run_id": course_run.get("courseRunId", ""),
                "trainer_id": trainer_dict.get("trainerId", ""),
                "trainer_id_number": trainer_dict.get("idNumber", ""),
                "trainer_id_type_code": trainer_dict.get("idTypeCode", ""),
                "trainer_uuid": trainer_dict.get("uuid", ""),
                "trainer_name": trainer_dict.get("name", ""),
                "trainer_email": trainer_dict.get("email", ""),
                "trainer_practice_area": trainer_dict.get("domainAreaOfPractice", ""),
                "trainer_qualification_level": trainer_dict.get(
                    "qualificationLevel", ""
                ),
                "trainer_experience": trainer_dict.get("experience", ""),
            }
            trainer_list.append(trainer_instance)

    return trainer_list


def parse_job_roles(course_detail_dict):
    """Parse job roles from course details, handling None values safely."""
    if not isinstance(course_detail_dict, dict):
        course_detail_dict = {}

    course_reference_number = course_detail_dict.get("courseReferenceNumber", "")
    relevant_job_roles = course_detail_dict.get("relevantJobRoles", "")

    if not isinstance(relevant_job_roles, str):
        relevant_job_roles = ""

    job_roles = [role.strip() for role in relevant_job_roles.split(",") if role.strip()]

    return [
        JobRoleCourseDetails(
            course_reference_number=course_reference_number, job_role=role
        )
        for role in job_roles
    ]


def parse_course_details(course_detail_dict):
    """Parse course details and return the CourseDetails dataclass, handling None values safely."""
    if not isinstance(course_detail_dict, dict):
        course_detail_dict = {}

    qualification_attained = course_detail_dict.get("qualificationAttained", {})
    if not isinstance(qualification_attained, dict):
        qualification_attained = {}

    course_details = CourseDetails(
        course_reference_number=course_detail_dict.get("courseReferenceNumber", ""),
        course_title=course_detail_dict.get("courseTitle", ""),
        course_objective=course_detail_dict.get("courseObjective", ""),
        course_content=course_detail_dict.get("courseContent", ""),
        entry_requirement=course_detail_dict.get("entryRequirement", ""),
        training_days=course_detail_dict.get("numberOfTrainingDay", ""),
        training_duration_hours=course_detail_dict.get("totalTrainingDurationHour", ""),
        count_attendees=course_detail_dict.get("courseAttendeeCount", ""),
        qualification_attained_id=qualification_attained.get(
            "qualificationAttainedCode", ""
        ),
        qualification_attained_name=qualification_attained.get("description", ""),
    )
    return course_details


import time


def get_all_courses_data(course_reference_numbers):
    all_courses = []
    all_trainers = []
    all_job_roles = []
    all_mode_of_trainings = []
    all_course_runs = []

    total_courses = len(course_reference_numbers)
    start_time = time.time()  # Track the start time

    for idx, course_reference in enumerate(course_reference_numbers):
        start_course_time = time.time()  # Track the time to parse each course
        response = fetch_course_details(course_reference)
        if response and response.status_code == 200 and "data" in response.json():
            course_detail_dict = response.json().get("data", {})

            # Parse course details and extract relevant data
            course_details = parse_course_details(course_detail_dict)
            all_courses.append(course_details)

            # Collect mode of trainings, course runs, trainers, and job roles
            mode_of_trainings = parse_mode_of_trainings(course_detail_dict)
            all_mode_of_trainings.extend(mode_of_trainings)

            course_runs = parse_course_runs(course_detail_dict)
            all_course_runs.extend(course_runs)

            trainers = parse_trainers(course_detail_dict)
            all_trainers.extend(trainers)

            job_roles = parse_job_roles(course_detail_dict)
            all_job_roles.extend(job_roles)

        # Calculate the time taken for this course
        course_time_taken = time.time() - start_course_time

        # Estimate remaining time based on the time taken so far
        courses_processed = idx + 1  # Courses processed so far
        courses_remaining = total_courses - courses_processed

        # Estimate time remaining
        if courses_processed > 1:  # Avoid division by zero for the first iteration
            average_time_per_course = (time.time() - start_time) / courses_processed
            estimated_time_remaining = average_time_per_course * courses_remaining
            estimated_completion_time = start_time + (
                average_time_per_course * total_courses
            )

            # Convert to human-readable format (minutes)
            estimated_time_remaining_minutes = estimated_time_remaining / 60
            estimated_completion_time_readable = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(estimated_completion_time)
            )

            print(
                f"Estimated time remaining: {estimated_time_remaining_minutes:.2f} minutes,\tcomplete at {estimated_completion_time_readable}"
            )
        else:
            print("Estimating remaining time...")

    # Convert all data to DataFrames
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


for course_references in course_reference_numbers_list:
    (
        course_details_df,
        trainers_df,
        job_roles_df,
        mode_of_trainings_df,
        course_runs_df,
    ) = get_all_courses_data(course_references)

    from datetime import datetime

    import pandas as pd
    import pandas_gbq

    # Get the current timestamp
    accessed_at_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Add the '_accessed_at' column to each DataFrame
    course_details_df["_accessed_at"] = accessed_at_timestamp
    trainers_df["_accessed_at"] = accessed_at_timestamp
    job_roles_df["_accessed_at"] = accessed_at_timestamp
    mode_of_trainings_df["_accessed_at"] = accessed_at_timestamp
    course_runs_df["_accessed_at"] = accessed_at_timestamp

    pandas_gbq.to_gbq(
        dataframe=course_details_df,
        destination_table="sg_skillsfuture.course_details",
        project_id="jeremy-chia",
        if_exists="append",
    )

    pandas_gbq.to_gbq(
        dataframe=trainers_df,
        destination_table="sg_skillsfuture.trainers",
        project_id="jeremy-chia",
        if_exists="append",
    )

    pandas_gbq.to_gbq(
        dataframe=job_roles_df,
        destination_table="sg_skillsfuture.job_roles",
        project_id="jeremy-chia",
        if_exists="append",
    )

    pandas_gbq.to_gbq(
        dataframe=mode_of_trainings_df,
        destination_table="sg_skillsfuture.mode_of_trainings",
        project_id="jeremy-chia",
        if_exists="append",
    )

    pandas_gbq.to_gbq(
        dataframe=course_runs_df,
        destination_table="sg_skillsfuture.course_runs",
        project_id="jeremy-chia",
        if_exists="append",
    )
