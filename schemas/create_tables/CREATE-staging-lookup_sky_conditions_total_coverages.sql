DROP TABLE IF EXISTS staging.lookup_sky_conditions_total_coverages; 
CREATE UNLOGGED TABLE staging.lookup_sky_conditions_total_coverages (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
