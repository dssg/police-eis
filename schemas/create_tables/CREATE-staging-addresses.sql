DROP TABLE IF EXISTS staging.addresses; 
CREATE UNLOGGED TABLE staging.addresses (
	address_id                                                            serial,                --
	department_defined_reference_id										  varchar,
	street_address                                                        text,               --street address 
	second_street_address                                                 text,               --unit/apartment
	city                                                                  text,               --city
	zip                                                                   int,                --zip code
	state                                                                 text,               --state
	latitude                                                              numeric,            --
	longitude                                                             numeric,            --
	hundreds_block                                                        text                --
);
