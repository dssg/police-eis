DROP TABLE IF EXISTS staging.lookup_traffic_stop_outcome_type; 
CREATE UNLOGGED TABLE staging.lookup_traffic_stop_outcome_type (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
