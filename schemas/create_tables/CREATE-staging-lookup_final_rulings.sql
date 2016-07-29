DROP TABLE IF EXISTS staging.lookup_final_rulings; 
CREATE UNLOGGED TABLE staging.lookup_final_rulings (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
