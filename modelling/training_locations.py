import os
import pandas_gbq 

project_id = "jeremy-chia"
target_schema = "jeremy-chia.sg_skillsfuture_models"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tokens/gcp_token.json"
sql = """

with
    course_runs as (

        select
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
            coalesce(address_block, "") as address_block,
            coalesce(address_street, "") as address_street,
            coalesce(address_floor, "") as address_floor,
            coalesce(address_unit, "") as address_unit,
            coalesce(address_building, "") as address_building,
            coalesce(address_postal_code, "") as address_postal_code,
            coalesce(address_room, "") as address_room,
            count(distinct course_reference_number) as count_courses,
            count(distinct course_run_id) as count_course_runs,
        from `jeremy-chia.sg_skillsfuture.course_runs`
        where
            address_block is not null
            and address_street is not null
            and address_floor is not null
            and address_unit is not null
            and address_building is not null
            and address_postal_code is not null
            and address_room is not null
        group by all
    ),

    addresses as (
        select postal_code, block_number, road_name, latitutde, longitude,
        from `jeremy-chia.sg_skillsfuture.addresses`
    ),

    joined as (
        select
            course_runs.address_room_uuid,
            course_runs.address_postal_code,
            coalesce(
                addresses.block_number, course_runs.address_block
            ) as address_block,
            coalesce(addresses.road_name, course_runs.address_street) as address_street,
            course_runs.address_building,
            course_runs.address_floor,
            course_runs.address_unit,
            course_runs.address_room,
            addresses.latitutde,
            addresses.longitude,
            course_runs.count_courses,
            course_runs.count_course_runs,
        from course_runs
        left join addresses on course_runs.address_postal_code = addresses.postal_code
    )

select *
from joined
order by address_postal_code, address_room


"""

df = pandas_gbq.read_gbq(
    query_or_table=sql,
    project_id=project_id
)

pandas_gbq.to_gbq(
    dataframe=df,
    destination_table=f"{target_schema}.training_locations",
    project_id=project_id,
    if_exists="replace"
)