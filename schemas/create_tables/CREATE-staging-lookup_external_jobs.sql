DROP TABLE IF EXISTS staging.lookup_external_jobs; 
CREATE UNLOGGED TABLE staging.lookup_external_jobs (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
