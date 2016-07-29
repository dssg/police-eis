DROP TABLE IF EXISTS staging.lookup_complaint_origins; 
CREATE UNLOGGED TABLE staging.lookup_complaint_origins (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
