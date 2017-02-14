DROP TABLE IF EXISTS staging.officer_injury; 
CREATE  TABLE staging.officer_injury (
	event_id                                                              int references staging.events_hub(event_id) on delete cascade,                --event id
	injury_type_code                                                      int,                --the type of injury
	department_defined_injury_id                                          varchar,                --exposure id
	injury_narrative                                                      text                --Officer's explanation of exposure
);
