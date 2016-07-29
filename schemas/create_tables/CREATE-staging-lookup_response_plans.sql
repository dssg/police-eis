DROP TABLE IF EXISTS staging.lookup_response_plans; 
CREATE UNLOGGED TABLE staging.lookup_response_plans (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
