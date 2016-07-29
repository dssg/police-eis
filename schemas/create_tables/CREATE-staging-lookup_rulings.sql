DROP TABLE IF EXISTS staging.lookup_rulings; 
CREATE UNLOGGED TABLE staging.lookup_rulings (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
