with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_labels') }}

),

renamed as (

    select
        label_id,
        label

    from source
    where label_id is not null
)

select * from renamed
