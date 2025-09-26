with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_organization_types') }}

),

renamed as (

    select
        organization_type_id,
        organization_type_name

    from source
    where organization_type_id is not null
)

select * from renamed
