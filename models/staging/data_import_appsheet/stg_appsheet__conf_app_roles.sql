with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_roles') }}

),

renamed as (

    select
        role_id,
        role_name

    from source
    where role_id is not null
)

select * from renamed
