with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_workshops') }}

),

renamed as (

    select
        workshop_id,
        conference_id,
        workshop_name,
        workshop_description,
        workshop_date,
        workshop_start_time,
        workshop_end_time

    from source
    where workshop_id is not null
)

select * from renamed
