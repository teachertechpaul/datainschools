with

persons as (
    select * from {{ ref('int_persons_joined') }}
),

dedup_conferences as (
    select distinct person_id, conference_id, conference_start_date
    from persons
),

calc_fields as (
    select
        person_id, conference_id, conference_start_date,
        count(conference_id) over (
            partition by person_id
            order by conference_start_date
            rows between unbounded preceding and current ROW
        ) as conferences_so_far
    from dedup_conferences
),

joined as (
    select
        persons.*,
        calc_fields.conferences_so_far
    from persons
    inner join calc_fields using (person_id, conference_id)
)

select * from joined