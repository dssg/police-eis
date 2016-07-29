DROP TABLE IF EXISTS staging.lookup_drug_types; 
CREATE UNLOGGED TABLE staging.lookup_drug_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
