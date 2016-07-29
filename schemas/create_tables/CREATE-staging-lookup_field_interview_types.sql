DROP TABLE IF EXISTS staging.lookup_field_interview_types; 
CREATE UNLOGGED TABLE staging.lookup_field_interview_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
