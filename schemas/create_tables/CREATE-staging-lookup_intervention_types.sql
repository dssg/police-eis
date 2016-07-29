DROP TABLE IF EXISTS staging.lookup_intervention_types; 
CREATE UNLOGGED TABLE staging.lookup_intervention_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
