DROP TABLE IF EXISTS staging.events_geoid;
CREATE  TABLE staging.events_geoid (
	event_id	            	bigint,
	geoid 				varchar --- geoid from Census 2010
);
