with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_conferences') }}

),

renamed as (

    select
        conference_id,
        conference_abbr,
        conference_name,
        PARSE_DATE('%m/%d/%Y',conference_start_date) as conference_start_date,
        PARSE_DATE('%m/%d/%Y',conference_end_date) as conference_end_date

    from source
    where conference_id is not null
)

select * from renamed
