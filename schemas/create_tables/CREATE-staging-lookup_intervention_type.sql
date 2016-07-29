DROP TABLE IF EXISTS staging.lookup_intervention_type; 
CREATE UNLOGGED TABLE staging.lookup_intervention_type (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
