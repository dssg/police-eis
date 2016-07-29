DROP TABLE IF EXISTS staging.lookup_marital_statuses; 
CREATE UNLOGGED TABLE staging.lookup_marital_statuses (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
