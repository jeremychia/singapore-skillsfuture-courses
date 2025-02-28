from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

@dataclass
class CourseInfo:
    course_uuid: str
    course_reference_number: str
    course_created_date: Optional[datetime]
    course_nearest_start_date: Optional[datetime]
    course_funding_method: str
    quality_count_respondents: str
    quality_rating_out_of_5: str
    course_title: str
    course_duration: str
    course_fees: str
    training_partner_name: str
    training_partner_uen: str
    training_partner_course_reference: str

    @staticmethod
    def from_dict(course_dict: dict) -> "CourseInfo":
        course_data = course_dict.get("doclist", {}).get("docs", [{}])[0]
        return CourseInfo(
            course_uuid=course_dict.get("groupValue", ""),
            course_reference_number=course_data.get("Course_Ref_No", ""),
            course_created_date=course_data.get("Course_Created_Date", ""),
            course_nearest_start_date=course_data.get("Course_Start_Date_Nearest", ""),
            course_funding_method=course_data.get("Course_Funding", ""),
            quality_count_respondents=course_data.get(
                "Course_Quality_NumberOfRespondents", ""
            ),
            quality_rating_out_of_5=course_data.get("Course_Quality_Stars_Rating", ""),
            course_title=course_data.get("Course_Title", ""),
            course_duration=course_data.get("Len_of_Course_Duration_facet", ""),
            course_fees=course_data.get("Tol_Cost_of_Trn_Per_Trainee", ""),
            training_partner_name=course_data.get("Organisation_Name", ""),
            training_partner_uen=course_data.get("UEN", ""),
            training_partner_course_reference=course_data.get("EXT_Course_Ref_No", ""),
        )


@dataclass
class TrainingArea:
    course_reference_number: str
    area_of_training_id: str
    area_of_training_text: str

    @staticmethod
    def from_course_dict(
        course_reference: str, course_data: dict
    ) -> List["TrainingArea"]:
        return [
            TrainingArea(course_reference, area_id, area_text)
            for area_id, area_text in zip(
                course_data.get("Area_of_Training", []),
                course_data.get("Area_of_Training_text", []),
            )
        ]


@dataclass
class LanguageOfInstruction:
    course_reference_number: str
    language_of_instruction: str


@dataclass
class FeaturedInitiatives:
    course_reference_number: str
    featured_initiatives_tag: str


@dataclass
class SkillsFutureInitiatives:
    course_reference_number: str
    skillsfuture_initiatives_tag: str
