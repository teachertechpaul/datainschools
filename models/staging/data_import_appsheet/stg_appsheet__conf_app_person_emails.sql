with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_person_emails') }}

),

renamed as (

    select
        email_id,
        person_id,
        person_email

    from source
    where email_id is not null
)

select * from renamed
