with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_person_roles') }}

),

renamed as (

    select
        person_role_id,
        person_id,
        conference_id,
        role_id

    from source
    where person_role_id is not null
)

select * from renamed
