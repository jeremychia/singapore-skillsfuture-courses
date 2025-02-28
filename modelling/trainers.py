import os
import pandas_gbq 

project_id = "jeremy-chia"
target_schema = "jeremy-chia.sg_skillsfuture_models"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tokens/gcp_token.json"
sql = """

with
    trainer_profile as (
        select
            trainer_uuid,
            first_value(trainer_id_number) over (
                partition by trainer_uuid order by _accessed_at desc
            ) as trainer_id_number,
            first_value(trainer_id_type_code) over (
                partition by trainer_uuid order by _accessed_at desc
            ) as trainer_id_type_code,
            first_value(trainer_name) over (
                partition by trainer_uuid order by _accessed_at desc
            ) as trainer_name,
            trainer_email,
            trainer_practice_area,
            trainer_experience,
            course_reference_number,
            course_run_id,
        from `jeremy-chia.sg_skillsfuture.trainers`
        where trainer_uuid is not null and trainer_uuid != ''
    ),

    trainer_profile_summarised as (
        select
            trainer_uuid,
            trainer_id_number,
            trainer_id_type_code,
            trainer_name,
            array_agg(distinct coalesce(trainer_email, "")) as emails,
            array_agg(distinct coalesce(trainer_practice_area, "")) as practice_areas,
            array_agg(distinct coalesce(trainer_experience, "")) as experiences,
            count(distinct course_reference_number) as count_courses,
            count(distinct course_run_id) as count_course_runs,
        from trainer_profile
        group by all
    )

select *
from trainer_profile_summarised


"""

df = pandas_gbq.read_gbq(
    query_or_table=sql,
    project_id=project_id
)

pandas_gbq.to_gbq(
    dataframe=df,
    destination_table=f"{target_schema}.trainers",
    project_id=project_id,
    if_exists="replace"
)