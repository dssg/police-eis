DROP TABLE IF EXISTS staging.field_interviews;
CREATE UNLOGGED TABLE staging.field_interviews (
  event_id                                  INT REFERENCES staging.events_hub (event_id), --not a primary key, multiple people could be involved in one interview
  department_defined_field_interview_id     VARCHAR, --the department's internal id for the interview
  field_interview_type_code                 INT, --type of field interview
  field_interview_outcome_code              INT, --outcomoe of field interview
  field_interview_day                       VARCHAR, --day of the week of the field interview
  interviewed_person_id                     INT, --person id of the person being interviewed
  interviewed_person_age_at_field_interview INT, --age (at the time of the interview) of the person being interviewed
  interviewed_person_race                   INT, --race of the person being interviewed
  interviewed_person_ethnicity              INT, --ethnicity of the person being interviewed
  interviewed_person_gender                 INT, --gender of the person being interviewed
  searched_flag                             BOOL, --was a search executed?
  search_justification_code                 INT, --justification for the search (driver consent)
  drugs_found_flag                          BOOL, --were drugs found during the search?
  drug_type_code                            INT, --what kind of drugs were found?
  weapons_found_flag                        BOOL, --were weapons found?
  weapon_type_code                          INT, --what kind of weapons were found
  event_datetime                            TIMESTAMP,
  officer_id                                INT REFERENCES staging.officers_hub (officer_id)
);
