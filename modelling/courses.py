import os
import pandas_gbq 

project_id = "jeremy-chia"
target_schema = "jeremy-chia.sg_skillsfuture_models"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tokens/gcp_token.json"
sql = """

with
    courses as (
        select
            course_reference_number,
            course_title,
            date(course_created_date) as course_created_date,
            course_fees as course_fees_sgd,
            course_funding_method,
            date(
                case
                    when course_nearest_start_date != "" then course_nearest_start_date
                end
            ) as course_nearest_start_date,
            course_duration,
            cast(
                case
                    when quality_count_respondents != "" then quality_count_respondents
                end as int
            ) as count_quality_respondents,
            cast(
                case
                    when quality_rating_out_of_5 != "" then quality_rating_out_of_5
                end as float64
            ) as quality_rating_out_of_5,
            training_partner_uen,
            training_partner_name,
            _accessed_at
        from `jeremy-chia.sg_skillsfuture.courses`
    ),

    details as (
        select
            course_reference_number,
            course_objective,
            course_content,
            entry_requirement,
            cast(training_duration_hours as float64) as training_duration_hours,
            count_attendees,
            case
                when qualification_attained_id != "" then qualification_attained_id
            end as qualification_attained_id,
            case
                when qualification_attained_name != "" then qualification_attained_name
            end as qualification_attained_name
        from `jeremy-chia.sg_skillsfuture.course_details`
        qualify
            row_number() over (
                partition by course_reference_number order by _accessed_at desc
            )
            = 1
    ),

    union_initiatives as (
        select distinct
            course_reference_number, featured_initiatives_tag as initiatives_tag
        from `jeremy-chia.sg_skillsfuture.featured_initiatives`
        union all
        select distinct
            course_reference_number, skillsfuture_initiatives_tag as initiatives_tag
        from `jeremy-chia.sg_skillsfuture.skillsfuture_initiatives`
    ),

    initiatives as (
        select
            course_reference_number,
            array_agg(
                distinct initiatives_tag order by initiatives_tag
            ) as initiatives_tags
        from union_initiatives
        group by all
    ),

    languages as (
        select
            course_reference_number,
            array_agg(
                distinct language_of_instruction order by language_of_instruction
            ) as languages_of_instruction,
            count(distinct language_of_instruction) as count_languages
        from `jeremy-chia.sg_skillsfuture.languages`
        group by all
    ),

    job_roles as (
        select
            course_reference_number,
            array_agg(distinct job_role order by job_role) as job_roles,
        from `jeremy-chia.sg_skillsfuture.job_roles`
        group by all
    ),

    training_areas as (
        select course_reference_number, area_of_training_id, area_of_training_text,
        from `jeremy-chia.sg_skillsfuture.training_areas`
        qualify
            row_number() over (
                partition by course_reference_number
                order by _accessed_at desc, area_of_training_id
            )
            = 1
    ),

    mode_of_training as (
        select course_reference_number, mode_of_training_description,
        from `jeremy-chia.sg_skillsfuture.mode_of_trainings`
        qualify
            row_number() over (
                partition by course_reference_number order by _accessed_at desc
            )
            = 1
    ),

    joined as (
        select
            coalesce(
                courses.course_reference_number, details.course_reference_number
            ) as course_reference_number,
            courses.course_created_date,

            courses.course_title,
            details.course_objective,
            details.course_content,

            courses.course_fees_sgd,
            courses.course_funding_method,

            courses.course_nearest_start_date,
            mode_of_training.mode_of_training_description,
            languages.languages_of_instruction,
            languages.count_languages,
            courses.course_duration,
            details.training_duration_hours,
            details.entry_requirement,
            details.qualification_attained_id,
            details.qualification_attained_name,
            training_areas.area_of_training_id,
            training_areas.area_of_training_text,
            job_roles.job_roles,

            details.count_attendees,
            courses.count_quality_respondents,
            courses.quality_rating_out_of_5,

            courses.training_partner_uen,
            courses.training_partner_name,

            initiatives.initiatives_tags,

        from courses
        full join
            details on courses.course_reference_number = details.course_reference_number
        full join
            initiatives
            on courses.course_reference_number = initiatives.course_reference_number
        full join
            languages
            on courses.course_reference_number = languages.course_reference_number
        full join
            job_roles
            on courses.course_reference_number = job_roles.course_reference_number
        full join
            training_areas
            on courses.course_reference_number = training_areas.course_reference_number
        full join
            mode_of_training
            on courses.course_reference_number
            = mode_of_training.course_reference_number
    )

select *
from joined
"""

df = pandas_gbq.read_gbq(
    query_or_table=sql,
    project_id=project_id
)

pandas_gbq.to_gbq(
    dataframe=df,
    destination_table=f"{target_schema}.courses",
    project_id=project_id,
    if_exists="replace"
)