DROP TABLE IF EXISTS staging.lookup_search_justifications; 
CREATE UNLOGGED TABLE staging.lookup_search_justifications (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
