DROP TABLE IF EXISTS staging.lookup_field_interview_outcomes; 
CREATE UNLOGGED TABLE staging.lookup_field_interview_outcomes (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
