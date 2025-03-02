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
            -- count(distinct course_reference_number) as count_courses,
            count(distinct course_run_id) as count_course_runs,
        from trainer_profile
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

    training_partners as (
        select distinct
            course_reference_number, training_partner_uen, training_partner_name,
        from `jeremy-chia.sg_skillsfuture.courses`
        where training_partner_uen is not null and training_partner_name is not null
    ),

    trainers_with_course_areas as (
        select distinct
            trainers.trainer_uuid,
            trainers.course_reference_number,
            training_areas.area_of_training_id,
            training_areas.area_of_training_text,
            training_partners.training_partner_uen,
            training_partners.training_partner_name,
        from `jeremy-chia.sg_skillsfuture.trainers` as trainers
        left join
            training_areas
            on trainers.course_reference_number = training_areas.course_reference_number
        left join
            training_partners
            on trainers.course_reference_number
            = training_partners.course_reference_number
        where trainer_uuid != '' and trainer_uuid is not null
    ),

    summarise_course_reference_numbers as (
        select
            trainer_uuid,
            array_agg(
                distinct course_reference_number order by course_reference_number
            ) as course_reference_numbers,
            count(distinct course_reference_number) as count_courses,
        from trainers_with_course_areas
        group by all
    ),

    distinct_training_areas as (
        select
            trainer_uuid,
            area_of_training_id,
            area_of_training_text,
            count(distinct course_reference_number) as count_by_area_of_training,
        from trainers_with_course_areas
        group by all
        order by area_of_training_id
    ),

    summarise_training_areas as (
        select
            trainer_uuid,
            max_by(
                area_of_training_id, count_by_area_of_training
            ) as primary_area_of_training_id,
            max_by(
                area_of_training_text, count_by_area_of_training
            ) as primary_area_of_training_text,
            array_agg(
                struct(
                    area_of_training_id,
                    area_of_training_text,
                    count_by_area_of_training
                )
            ) as areas_of_training,
            count(distinct area_of_training_id) as count_areas_of_training,
        from distinct_training_areas
        group by all
    ),

    distinct_training_partners as (
        select distinct trainer_uuid, training_partner_uen, training_partner_name
        from trainers_with_course_areas
        order by training_partner_uen
    ),

    summarise_training_partners as (
        select
            trainer_uuid,
            array_agg(
                struct(training_partner_uen, training_partner_name)
            ) as training_partners,
            count(distinct training_partner_uen) as count_training_partners,
        from distinct_training_partners
        group by all
    ),

    joined as (
        select
            trainer_profile.trainer_uuid,
            trainer_profile.trainer_id_number,
            trainer_profile.trainer_id_type_code,
            trainer_profile.trainer_name,
            trainer_profile.emails,

            summarise_training_partners.training_partners,
            summarise_training_partners.count_training_partners,

            trainer_profile.practice_areas,
            trainer_profile.experiences,
            summarise_training_areas.primary_area_of_training_id,
            summarise_training_areas.primary_area_of_training_text,
            summarise_training_areas.count_areas_of_training,
            summarise_training_areas.areas_of_training,

            summarise_course_reference_numbers.count_courses,
            trainer_profile.count_course_runs,
            summarise_course_reference_numbers.course_reference_numbers,

        from trainer_profile_summarised as trainer_profile
        left join
            summarise_course_reference_numbers
            on trainer_profile.trainer_uuid
            = summarise_course_reference_numbers.trainer_uuid
        left join
            summarise_training_areas
            on trainer_profile.trainer_uuid = summarise_training_areas.trainer_uuid
        left join
            summarise_training_partners
            on trainer_profile.trainer_uuid = summarise_training_partners.trainer_uuid

    ),

    calculate_year_of_birth as (
        select
            *,
            case
                -- for NRICs starting with 'S', only 1968 and onwards has the number
                when
                    length(trainer_id_number) = 9
                    and upper(trainer_id_number) like 'S%'
                    and safe_cast(substr(trainer_id_number, 2, 2) as int) >= 68
                then concat('19', substr(trainer_id_number, 2, 2))
                -- NRICs starting with 'T' is for 2000 and after
                when
                    length(trainer_id_number) = 9 and upper(trainer_id_number) like 'T%'
                then concat('20', substr(trainer_id_number, 2, 2))
            end as year_of_birth
        from joined
    ),

    calculate_age as (
        select
            *,
            case
                when year_of_birth is not null
                then
                    cast(
                        cast(extract(year from current_date()) as int)
                        - cast(year_of_birth as int) as string
                    )
                when
                    trainer_id_number like 'S%'
                    and safe_cast(substr(trainer_id_number, 2, 2) as int) < 68
                then
                    concat(
                        cast(
                            cast(extract(year from current_date()) as int)
                            - 1967 as string
                        ),
                        " or older"
                    )
                else "Unknown"
            end as age,

        from calculate_year_of_birth
    )

select *
from calculate_age
order by count_areas_of_training desc


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