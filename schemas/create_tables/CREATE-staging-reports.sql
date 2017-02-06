DROP TABLE IF EXISTS staging.reports; 
CREATE  TABLE staging.reports (
	event_id                                                              int references staging.events_hub(event_id),                --Primary key
	department_defined_report_id                                          varchar,            --
	report_type_code                                                      int,                --
	department_defined_complaint_number                                   varchar,            --
	interview_in_person                                                   bool,               --In person or on phone?
	traffic_stop                                                          bool,               --Report from traffic stop
	report_narrative                                                      text,               --
	report_date                                                           date                --
);
