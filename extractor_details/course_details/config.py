# Project Configuration
PROJECT_ID = "jeremy-chia"
BASE_URL = "https://www.myskillsfuture.gov.sg/services/tex/individual/course-detail"
COURSE_DETAIL_URL_TEMPLATE = "https://www.myskillsfuture.gov.sg/content/portal/en/training-exchange/course-directory/course-detail.html?courseReferenceNumber={}"
CHUNK_SIZE = 1000
PRIMARY_KEY = {
    "sg_skillsfuture.course_details": ["course_reference_number"],
    "sg_skillsfuture.trainers": ["course_run_id", "trainer_id_number", "trainer_uuid"],
    "sg_skillsfuture.job_roles": ["course_reference_number", "job_role"],
    "sg_skillsfuture.mode_of_trainings": [
        "course_reference_number",
        "mode_of_training_description",
    ],
    "sg_skillsfuture.course_runs": ["course_run_id"],
}
