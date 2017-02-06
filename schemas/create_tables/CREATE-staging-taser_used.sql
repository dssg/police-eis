DROP TABLE IF EXISTS staging.taser_used; 
CREATE  TABLE staging.taser_used (
	event_id                                                              int references staging.events_hub(event_id),                --
	department_defined_taser_id                                           varchar,                --Native key
	department_defined_internal_affairs_id                                int,                --Internal affairs allegation id
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
