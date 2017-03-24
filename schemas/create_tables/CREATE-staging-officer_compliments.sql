DROP TABLE IF EXISTS staging.officer_compliments; 
CREATE  TABLE staging.officer_compliments (
    event_id                INT references staging.events_hub(event_id) ON DELETE cascade,
    officer_id              INT REFERENCES staging.officers_hub (officer_id),
    event_datetime          TIMESTAMP,
    compliment_id           VARCHAR(50),
    method_sent             VARCHAR(50),
    synopsis                TEXT
);
