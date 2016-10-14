DROP TABLE IF EXISTS staging.complaints; 
CREATE UNLOGGED TABLE staging.complaints (
	event_id                                                              int references staging.events_hub(event_id),                --event that the complaint maps to in the events hub
	complaint_id                                                          int,                --the unique id of the complaint. If there are multiple officers for the same complaint, they will share the same id. The pk of this table is the combination of complaint_id and accused_officer_id
	accusing_officer_id                                                   int,                --the officer who is doing the complaining, if internal
	grouped_incident_type_code                                            int,                --the type of incident being complained about e.g. uniform, profanity
	complaint_narrative                                                   text,               --raw text describing the complaint
	datetime                                                              date,               --the datetime the complaint was filed
	department_defined_complaint_id                                       varchar,                --mapping to the original complaint id
	processed_flag                                                        bool,               --whether or not the complaint has been processed yet. If the complaint_id is present in the internal_affairs_investigations.complaint_id column, then it is considered processed
	complaint_origin_code                                                 int,                --whether complaint is internal or external
	accusing_citizen_hash                                                 int,                --may chuck this. This is a unique id for the member of the public that filed a complaint.
	accusing_citizen_race_code                                            int,                --
	accusing_citizen_ethnicity_code                                       int,                --
	accusing_citizen_gender_code                                          int,                --
	accusing_citizen_age_at_complaint                                     int                 --
);
