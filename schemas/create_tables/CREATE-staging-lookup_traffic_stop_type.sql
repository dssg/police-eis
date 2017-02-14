DROP TABLE IF EXISTS staging.lookup_traffic_stop_type; 
CREATE  TABLE staging.lookup_traffic_stop_type (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
