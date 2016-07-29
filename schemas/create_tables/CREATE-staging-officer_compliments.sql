DROP TABLE IF EXISTS staging.officer_compliments; 
CREATE UNLOGGED TABLE staging.officer_compliments (
    event_id                int references staging.events_hub(event_id) ON DELETE cascade,
    compliment_id           VARCHAR(50),
	last_modified           timestamp,           
    method_sent             VARCHAR(50),
    synopsis                text
);
