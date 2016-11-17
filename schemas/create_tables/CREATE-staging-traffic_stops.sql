DROP TABLE IF EXISTS staging.traffic_stops;
CREATE UNLOGGED TABLE staging.traffic_stops (
  event_id                       INT PRIMARY KEY REFERENCES staging.events_hub (event_id), --Primary key
  department_defined_stop_id     VARCHAR, --stop id from the department
  stop_type_code                 INT, --type of stop
  stop_outcome_code              INT, --outcome of the stop
  stop_day                       TEXT, --day of the week of the stop
  stopped_person_id              INT, --person id of the person stopped
  stopped_person_state           VARCHAR(2), --
  stopped_person_driver_flag     BOOL, --
  stopped_person_age_at_stop     INT, --age (at the time of the stop) of the person stopped
  stopped_person_race_code       INT, --race of the person stopped
  stopped_person_ethnicity       INT, --ethniciy of the person stopped
  stopped_person_gender          INT, --gender of the person stopped
  search_consent_request_flag    BOOL, --was search consent requested?
  searched_flag                  BOOL, --was a search executed?
  search_justification_code      INT, --justification for the search (driver consent)
  search_justification_narrative TEXT, --narrative field for the search justification
  drugs_found_flag               BOOL, --were drugs found during the search?
  drug_type_code                 INT, --what kind of drugs were found?
  weapons_found_flag             BOOL, --were weapons found?
  arrest_flag                    BOOL, --was there an arrest following/during the stop
  stop_address_id                INT, --
  stopped_vehicle_code           INT, --kind of vehicle that was stopped
  stop_narrative                 TEXT, --narrative description of the stop
  injuries_flag                  BOOL, --were there injuries to the driver or passengers?
  officer_injury_flag            BOOL, --was there an injury to the officer?
  use_of_force_flag              BOOL, --was force used during the stop?
  event_datetime                 TIMESTAMP,
  officer_id                     INT REFERENCES staging.officers_hub (officer_id)
);
