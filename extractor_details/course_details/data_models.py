from dataclasses import dataclass


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
