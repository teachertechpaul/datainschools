with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_person_conferences') }}

),

renamed as (

    select
        person_conference_id,
        person_id,
        conference_id

    from source
    where person_conference_id is not null
)

select * from renamed
