import os
import pandas_gbq 

project_id = "jeremy-chia"
target_schema = "jeremy-chia.sg_skillsfuture_models"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tokens/gcp_token.json"
sql = """
with
    details as (
        select course_reference_number, count_attendees,
        from `jeremy-chia.sg_skillsfuture.course_details`
        qualify
            row_number() over (
                partition by course_reference_number order by _accessed_at desc
            )
            = 1
    ),
    course_information as (
        select
            courses.training_partner_uen,
            courses.training_partner_name,
            courses.course_reference_number,
            cast(
                case
                    when courses.quality_count_respondents != ""
                    then courses.quality_count_respondents
                end as int
            ) as count_quality_respondents,
            cast(
                case
                    when courses.quality_rating_out_of_5 != ""
                    then courses.quality_rating_out_of_5
                end as float64
            ) as quality_rating_out_of_5,
            courses.course_fees,
            date(courses.course_created_date) as course_created_date,
            details.count_attendees,

        from `jeremy-chia.sg_skillsfuture.courses` as courses
        left join
            details on courses.course_reference_number = details.course_reference_number
    ),

    summarise_by_training_partner as (
        select
            training_partner_uen,
            training_partner_name,

            count(distinct course_reference_number) as count_courses,
            sum(count_attendees) as count_attendees,
            sum(count_quality_respondents) as count_quality_respondents,
            sum(count_quality_respondents * quality_rating_out_of_5)
            / sum(count_quality_respondents) as average_rating_out_of_5,
            avg(course_fees) as average_course_fees_sgd,
            min(course_created_date) as earliest_course_created_date,
            max(course_created_date) as latest_course_created_date,
        from course_information
        group by all
    )

select *
from summarise_by_training_partner
order by count_attendees desc
"""

df = pandas_gbq.read_gbq(
    query_or_table=sql,
    project_id=project_id
)

pandas_gbq.to_gbq(
    dataframe=df,
    destination_table=f"{target_schema}.training_providers",
    project_id=project_id,
    if_exists="replace"
)