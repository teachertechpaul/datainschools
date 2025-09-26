with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_workshop_presenters') }}

),

renamed as (

    select
        workshop_id,
        person_id

    from source
    where workshop_id is not null
)

select * from renamed
