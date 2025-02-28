import os
import pandas_gbq 

project_id = "jeremy-chia"
target_schema = "jeremy-chia.sg_skillsfuture_models"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tokens/gcp_token.json"
sql = """
with
    course_runs as (

        select
            course_reference_number,
            course_run_id,
            date(course_run_start_date) as course_run_start_date,
            date(course_run_end_date) as course_run_end_date,
            date(registration_start_date) as registration_start_date,
            date(registration_end_date) as registration_end_date,
            course_run_training_mode,
            course_intake_size,
            if(
                address_block is not null
                and address_street is not null
                and address_floor is not null
                and address_unit is not null
                and address_building is not null
                and address_postal_code is not null
                and address_room is not null,
                md5(
                    concat(
                        coalesce(address_block, ""),
                        coalesce(address_street, ""),
                        coalesce(address_floor, ""),
                        coalesce(address_unit, ""),
                        coalesce(address_building, ""),
                        coalesce(address_postal_code, ""),
                        coalesce(address_room, "")
                    )
                ),
                null
            ) as address_room_uuid,
        from `jeremy-chia.sg_skillsfuture.course_runs`
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

    course as (
        select course_reference_number, training_partner_uen, training_partner_name
        from `jeremy-chia.sg_skillsfuture.courses`
        qualify
            row_number() over (
                partition by course_reference_number order by _accessed_at desc
            )
            = 1
    ),

    joined as (
        select
            course_runs.course_run_id,

            course_runs.course_reference_number,
            training_areas.area_of_training_id,
            training_areas.area_of_training_text,
            course_runs.course_run_start_date,
            course_runs.course_run_end_date,
            course_runs.registration_start_date,
            course_runs.registration_end_date,
            course_runs.course_run_training_mode,
            course_runs.course_intake_size,

            course_runs.address_room_uuid,
            course.training_partner_uen,
            course.training_partner_name,

        from course_runs
        left join
            course
            on course_runs.course_reference_number = course.course_reference_number
        left join
            training_areas
            on course_runs.course_reference_number
            = training_areas.course_reference_number

    )

select *
from joined
order by course_reference_number desc, course_run_id
"""

df = pandas_gbq.read_gbq(
    query_or_table=sql,
    project_id=project_id
)

pandas_gbq.to_gbq(
    dataframe=df,
    destination_table=f"{target_schema}.course_runs",
    project_id=project_id,
    if_exists="replace"
)