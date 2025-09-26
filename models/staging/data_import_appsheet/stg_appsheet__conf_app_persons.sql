with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_persons') }}

),

renamed as (

    select
        person_id,
        person_full_name,
        person_photo_url,
        organization_id,
        role_name as person_job_position,
        SAFE_CAST(person_can_be_emailed as BOOL) as person_can_be_emailed,
        SAFE_CAST(person_is_hidden as BOOL) as person_is_hidden

    from source
    where person_id is not null
)

select * from renamed
