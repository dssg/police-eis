DROP TABLE IF EXISTS staging.department_eis_alerts; 
CREATE UNLOGGED TABLE staging.department_eis_alerts (
	eis_alert_id                            serial primary key,     -- unique id for the eis alert
    officer_id                              int,                    -- unique id for the officer flagged by the (old) EIS system
    date_created                            timestamp,              -- date when the EIS alert was generated
    event_type                              varchar(50),            -- the type of threshold that was passed
    threshold                               int,                    -- the threshold value
    employee_count                          int,                    -- the officer's count which was >= the threshold
    intervention                            varchar(50),            -- the intervention assigned to the officer as a result of the alert
    notes                                   text,                  -- narrative text from the supervisor evaluating the alert
    status                                  varchar(13),            -- whether the EIS alert has been closed
    department_defined_eis_id               varchar(10)             -- the department ID. EISAutoNo for cmpd. MAY BE NONUNIQUE.
);
