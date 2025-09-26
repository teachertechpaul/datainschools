with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_person_notes') }}

),

renamed as (

    select
        person_note_id,
        person_id,
        note

    from source 
    where person_note_id is not null
)

select * from renamed
