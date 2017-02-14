DROP TABLE IF EXISTS staging.use_of_force;
CREATE  TABLE staging.use_of_force (
  event_id                                        INT REFERENCES staging.events_hub (event_id), --
  department_defined_use_of_force_id              VARCHAR, --Native key
  department_defined_internal_affairs_id          VARCHAR, --Internal affairs allegation id
  use_of_force_type_code                          INT, --type of force used
  in_response_to_resisting_arrest                 BOOL, --was the force in response to a suspect resisting arrest?
  suspect_injury                                  BOOL, --was the suspect injured? (This could also be type: minor/major?)
  suspect_person_id                               INT, --ID of person arrested
  suspect_age_at_arrest                           INT, --Age at time of arrest
  suspect_race_code                               INT, --race of suspect
  suspect_ethnicity_code                          INT, --ethnicity of suspect
  suspect_gender_code                             INT, --gender of suspect
  suspect_height_inches                           INT, --height of suspect in inches
  suspect_weight_pounds                           INT, --weight of suspect in pounds
  suspect_hair_color_code                         INT, --hair color of suspect
  suspect_eye_color_code                          INT, --eye color of suspect
  application_distance_meters                     INT, --Distance in meters that taser was fired from
  probe_penetration_flag                          BOOL, --Flag for probe penetrating suspect
  no_of_probes                                    INT, --Number of probes in taser
  reason_no_additional_cartridges_fired_narrative TEXT, --Officer's explanation why no additional cartridges fired
  subject_action_narrative                        TEXT, --Officer's explanation of what happened to subjected after taser fired
  additional_factors_narrative                    TEXT, --Additional information that officer provides explaining the success of tazer
  restrained_narrative                            TEXT, --Officer's description of if suspect was restrained
  state_discharge_narrative                       TEXT, --Officer's narrative of the state of subject after taser fired
  subject_injuries_narrative                      TEXT, --Officer's description of any injuries to the subject
  post_use_care_narrative                         TEXT, --Officer's description of medical care subject recieved after taser used
  recovery_time_minutes                           INT, --Officer's evaluation of time it took for subject to recover from taser
  event_datetime                                  TIMESTAMP,
  officer_id                                      INT REFERENCES staging.officers_hub (officer_id)
);
