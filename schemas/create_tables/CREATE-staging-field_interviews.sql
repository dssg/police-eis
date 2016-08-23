DROP TABLE IF EXISTS staging.field_interviews; 
CREATE UNLOGGED TABLE staging.field_interviews (
	event_id                                                              int references staging.events_hub(event_id),                --Primary key
	department_defined_field_interview_id                                 varchar,                --the department's internal id for the interview
	field_interview_type_code                                             int,                --type of field interview
	field_interview_outcome_code                                          int,                --outcomoe of field interview
	field_interview_day                                                   varchar,            --day of the week of the field interview
	interviewed_person_id                                                 int,                --person id of the person being interviewed
	interviewed_person_age_at_field_interview                             int,                --age (at the time of the interview) of the person being interviewed
	interviewed_person_race                                               int,                --race of the person being interviewed
	interviewed_person_ethnicity                                          int,                --ethnicity of the person being interviewed
	interviewed_person_gender                                             int,                --gender of the person being interviewed
	searched_flag                                                         bool,               --was a search executed?
	search_justification_code                                             int,                --justification for the search (driver consent)
	drugs_found_flag                                                      bool,               --were drugs found during the search?
	drug_type_code                                                        int,                --what kind of drugs were found?
	weapons_found_flag                                                    bool,               --were weapons found?
	weapon_type_code                                                      int                 --what kind of weapons were found
);
