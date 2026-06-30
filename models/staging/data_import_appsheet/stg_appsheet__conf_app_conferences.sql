with 

source as (

    select * from {{ source('data_import_appsheet', 'conf_app_conferences') }}

),

renamed as (

    select
        conference_id,
        conference_abbr,
        conference_name,
        COALESCE(
        SAFE.PARSE_DATE('%m/%d/%Y', conference_start_date),
        SAFE.PARSE_DATE('%Y-%m-%d', conference_start_date)
        ) as conference_start_date,
        COALESCE(
        SAFE.PARSE_DATE('%m/%d/%Y', conference_end_date),
        SAFE.PARSE_DATE('%Y-%m-%d', conference_end_date)
        ) as conference_end_date

    from source
    where conference_id is not null
),

add_fields as (
    select *,
        row_number() over (
            order by conference_start_date asc, conference_id
        ) as conference_number
    from renamed
)

select * from add_fields
