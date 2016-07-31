DROP TABLE IF EXISTS staging.dispatches; 
CREATE UNLOGGED TABLE staging.dispatches (
	event_id                                                              int references staging.events_hub(event_id),                --Primary key
	department_defined_dispatch_id                                        varchar,                --
	dispatch_address_id                                                   int,                --
	police_area_id                                                        int,                --
	dispatch_type_code                                                    int,                --the reason for the dispatch
	response_plan_code                                                    int,                --result of the dispatch
	dispatch_original_priority_code                                       int,                --
	dispatch_final_priority_code                                          int,                --
	travel_time_minutes                                                   int,                --
	response_time_minutes                                                 int,                --
	time_on_scene_minutes                                                 int,                --
	datetime_assigned                                                     timestamp,          --
	datetime_arrived                                                      timestamp,          --
	datetime_cleared                                                      timestamp           --
);