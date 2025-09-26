with

persons as (
    select * from {{ ref('stg_appsheet__conf_app_persons') }}
),

organizations as (
    select * from {{ ref('stg_appsheet__conf_app_organizations') }}
),

organization_types as (
    select * from {{ ref('stg_appsheet__conf_app_organization_types') }}
),

person_emails as (
    select * from {{ ref('stg_appsheet__conf_app_person_emails') }}
),

person_labels as (
    select * from {{ ref('stg_appsheet__conf_app_person_labels') }}
),

labels as (
    select * from {{ ref('stg_appsheet__conf_app_labels') }}
),

person_notes as (
    select * from {{ ref('stg_appsheet__conf_app_person_notes') }}
),

person_roles as (
    select * from {{ ref('stg_appsheet__conf_app_person_roles') }}
),

roles as (
    select * from {{ ref('stg_appsheet__conf_app_roles') }}
),

person_conferences as (
    select * from {{ ref('stg_appsheet__conf_app_person_conferences') }}
),

conferences as (
    select * from {{ ref('stg_appsheet__conf_app_conferences') }}
),

joined as (
    select
        TO_HEX(MD5(CONCAT(
            COALESCE(persons.person_id,''),
            COALESCE(person_emails.email_id,''),
            COALESCE(labels.label_id,''),
            COALESCE(person_notes.person_note_id,''),
            COALESCE(roles.role_id,''),
            COALESCE(conferences.conference_id,'')
        ))) as person_attribute_id,
        persons.*,
        organizations.* except (organization_id),
        organization_types.* except (organization_type_id),
        person_emails.* except (person_id),
        labels.*,
        person_notes.* except (person_id),
        roles.*,
        conferences.*
        
    from persons
    left outer join organizations
        on organizations.organization_id = persons.organization_id
    left outer join organization_types
        on organization_types.organization_type_id = organizations.organization_type_id
    left outer join person_emails
        on person_emails.person_id = persons.person_id
    left outer join person_labels
        on person_labels.person_id = persons.person_id
    left outer join labels
        on labels.label_id = person_labels.label_id
    left outer join person_notes
        on person_notes.person_id = persons.person_id
    left outer join person_roles
        on person_roles.person_id = persons.person_id
    left outer join roles
        on roles.role_id = person_roles.role_id
    left outer join person_conferences
        on person_conferences.person_id = persons.person_id
    left outer join conferences
        on conferences.conference_id = person_conferences.conference_id
)

select * from joined