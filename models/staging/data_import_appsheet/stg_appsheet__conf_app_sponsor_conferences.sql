with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_sponsor_conferences') }}

),

renamed as (

    select
        sponsor_conference_id,
        sponsor_id,
        conference_id

    from source
    where sponsor_conference_id is not null
)

select * from renamed
