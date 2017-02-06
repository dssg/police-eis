DROP TABLE IF EXISTS staging.officers_hub CASCADE; 
CREATE  TABLE staging.officers_hub (
	officer_id                                                            serial primary key, --officer id
	department_defined_officer_id                                         text,               --the department's internal officer identifier
	department_defined_officer_id_code									  text,				  --the department's internal officer ID code
	race_code                                                             int,                --officer race
	ethnicity_code                                                        int,                --whether the officer is hispanic
	date_of_birth                                                         timestamp,          --date of birth
	birth_city                                                            varchar,            --officer city of birth
	birth_state                                                           varchar,            --officer state of birth
	create_time                                                           timestamp,          --datetime when data was created
	last_modified                                                         timestamp           --datetime when data was last modified/updated
);
