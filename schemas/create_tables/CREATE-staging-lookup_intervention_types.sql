DROP TABLE IF EXISTS staging.lookup_intervention_types; 
CREATE  TABLE staging.lookup_intervention_types (
	code                                                                  int primary key,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
