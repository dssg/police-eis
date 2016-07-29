DROP TABLE IF EXISTS staging.officer_outside_employment; 
CREATE UNLOGGED TABLE staging.officer_outside_employment (
	officer_id                                                            int references staging.officers_hub(officer_id) on delete cascade,                --officer id
	external_job_code                                                     int,                --job that the officer had outside of the police force. e.g. security guard
	date_time                                                             timestamp,          --datetime that the job started
	hours_on_shift                                                        float,              --number of hours the officer worked
	in_uniform_flag                                                       bool,               --whether the officer was in police uniform when at this job i.e. were they in the role of a police officer
	address_raw                                                           varchar,            --address of where the job was
	address_id                                                            int                 --link to addresses table
);
