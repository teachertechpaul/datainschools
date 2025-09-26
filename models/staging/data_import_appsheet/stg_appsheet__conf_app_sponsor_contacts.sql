with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_sponsor_contacts') }}

),

renamed as (

    select
        sponsor_id,
        person_id

    from source
    where sponsor_id is not null
)

select * from renamed
