with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_person_labels') }}

),

renamed as (

    select
        person_label_id,
        person_id,
        label_id

    from source
    where person_label_id is not null
)

select * from renamed
