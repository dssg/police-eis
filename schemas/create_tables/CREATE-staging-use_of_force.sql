DROP TABLE IF EXISTS staging.use_of_force; 
CREATE UNLOGGED TABLE staging.use_of_force (
	event_id                                                              int references staging.events_hub(event_id),                --
	department_defined_use_of_force_id                                    varchar,                --Native key
	department_defined_internal_affairs_id                                varchar,                --Internal affairs allegation id
	use_of_force_type_code                                                int,                --type of force used
	in_response_to_resisting_arrest                                       bool,               --was the force in response to a suspect resisting arrest?
	suspect_injury                                                        bool,               --was the suspect injured? (This could also be type: minor/major?)
	suspect_person_id                                                     int,                --ID of person arrested
	suspect_age_at_arrest                                                 int,                --Age at time of arrest
	suspect_race_code                                                     int,                --race of suspect
	suspect_ethnicity_code                                                int,                --ethnicity of suspect
	suspect_gender_code                                                   int,                --gender of suspect
	suspect_height_inches                                                 int,                --height of suspect in inches
	suspect_weight_pounds                                                 int,                --weight of suspect in pounds
	suspect_hair_color_code                                               int,                --hair color of suspect
	suspect_eye_color_code                                                int,                --eye color of suspect
	application_distance_meters                                           int,                --Distance in meters that taser was fired from
	probe_penetration_flag                                                bool,               --Flag for probe penetrating suspect
	no_of_probes                                                          int,                --Number of probes in taser
	reason_no_additional_cartridges_fired_narrative                       text,               --Officer's explanation why no additional cartridges fired
	subject_action_narrative                                              text,               --Officer's explanation of what happened to subjected after taser fired
	additional_factors_narrative                                          text,               --Additional information that officer provides explaining the success of tazer
	restrained_narrative                                                  text,               --Officer's description of if suspect was restrained
	state_discharge_narrative                                             text,               --Officer's narrative of the state of subject after taser fired
	subject_injuries_narrative                                            text,               --Officer's description of any injuries to the subject
	post_use_care_narrative                                               text,               --Officer's description of medical care subject recieved after taser used
	recovery_time_minutes                                                 int                 --Officer's evaluation of time it took for subject to recover from taser
);
