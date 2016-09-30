DROP TABLE IF EXISTS staging.traffic_stops; 
CREATE UNLOGGED TABLE staging.traffic_stops (
	event_id                                                              int references staging.events_hub(event_id),                --Primary key
	department_defined_stop_id                                            varchar,                --stop id from the department
	stop_type_code                                                        int,                --type of stop
	stop_outcome_code                                                     int,                --outcome of the stop
	stop_day                                                              text,               --day of the week of the stop
	stopped_person_id                                                     int,                --person id of the person stopped
	stopped_person_state                                                  varchar(2),         --
	stopped_person_driver_flag                                            bool,               --
	stopped_person_age_at_stop                                            int,                --age (at the time of the stop) of the person stopped
	stopped_person_race_code                                              int,                --race of the person stopped
	stopped_person_ethnicity                                              int,                --ethniciy of the person stopped
	stopped_person_gender                                                 int,                --gender of the person stopped
	search_consent_request_flag                                           bool,               --was search consent requested?
	searched_flag                                                         bool,               --was a search executed?
	search_justification_code                                             int,                --justification for the search (driver consent)
	search_justification_narrative                                        text,               --narrative field for the search justification
	drugs_found_flag                                                      bool,               --were drugs found during the search?
	drug_type_code                                                        int,                --what kind of drugs were found?
	weapons_found_flag                                                    bool,               --were weapons found?
	arrest_flag                                                           bool,               --was there an arrest following/during the stop
	stop_address_id                                                       int,                --
	stopped_vehicle_code                                                  int,                --kind of vehicle that was stopped
	stop_narrative                                                        text,               --narrative description of the stop
    injuries_flag                                                         bool,               --were there injuries to the driver or passengers?
    officer_injury_flag                                                   bool,               --was there an injury to the officer?
    use_of_force_flag                                                     bool                --was force used during the stop?
);
