DROP TABLE IF EXISTS staging.lookup_rules_violated; 
CREATE UNLOGGED TABLE staging.lookup_rules_violated (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
