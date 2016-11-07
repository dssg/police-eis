DROP TABLE IF EXISTS staging.events_geoid;
CREATE UNLOGGED TABLE staging.events_geoid (
	event_id	            	bigint,
	geoid 				varchar --- geoid from Census 2010
);
