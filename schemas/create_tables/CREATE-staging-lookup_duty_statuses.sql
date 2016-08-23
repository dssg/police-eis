DROP TABLE IF EXISTS staging.lookup_duty_statuses; 
CREATE UNLOGGED TABLE staging.lookup_duty_statuses (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
