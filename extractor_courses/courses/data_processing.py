# --- data_processing.py ---
from typing import List

import pandas as pd
from courses.data_models import (
    CourseInfo,
    FeaturedInitiatives,
    LanguageOfInstruction,
    SkillsFutureInitiatives,
    TrainingArea,
)


def parse_response_to_dataframes(
    course_docs_list: List[dict],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    courses = [CourseInfo.from_dict(course_dict) for course_dict in course_docs_list]
    training_areas = []
    languages = []
    featured_initiatives = []
    skillsfuture_initiatives = []

    for course_info, course_dict in zip(courses, course_docs_list):
        course_data = course_dict.get("doclist", {}).get("docs", [{}])[0]
        training_areas.extend(
            TrainingArea.from_course_dict(
                course_info.course_reference_number, course_data
            )
        )
        languages.extend(
            LanguageOfInstruction(course_info.course_reference_number, language.strip())
            for language in course_data.get("Medium_of_Instruction_text", [])
        )
        featured_initiatives.extend(
            FeaturedInitiatives(course_info.course_reference_number, tag)
            for tag in course_data.get("Tags_text_FeaturedInitiatives", [])
        )
        skillsfuture_initiatives.extend(
            SkillsFutureInitiatives(course_info.course_reference_number, tag)
            for tag in course_data.get("Tags_text_SFInitiatives", [])
        )

    return (
        pd.DataFrame([course.__dict__ for course in courses]),
        pd.DataFrame([ta.__dict__ for ta in training_areas]),
        pd.DataFrame([lang.__dict__ for lang in languages]),
        pd.DataFrame([fi.__dict__ for fi in featured_initiatives]),
        pd.DataFrame([si.__dict__ for si in skillsfuture_initiatives]),
    )
