with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_organizations') }}

),

renamed as (

    select
        organization_id,
        organization_type_id,
        organization_name

    from source
    where organization_id is not null
)

select * from renamed
