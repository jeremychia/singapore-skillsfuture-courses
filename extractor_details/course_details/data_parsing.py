from course_details.data_models import (
    CourseDetails,
    CourseRun,
    JobRoleCourseDetails,
    ModeOfTraining,
    Trainer,
)


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
