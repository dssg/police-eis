DROP TABLE IF EXISTS staging.lookup_drug_types; 
CREATE  TABLE staging.lookup_drug_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
