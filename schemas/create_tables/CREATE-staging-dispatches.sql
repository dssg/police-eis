DROP TABLE IF EXISTS staging.dispatches; 
CREATE UNLOGGED TABLE staging.dispatches (
	event_id                                                              int references staging.events_hub(event_id),                --Primary key
	department_defined_dispatch_id                                        varchar,                --
	dispatch_address_id                                                   int,                --
	police_area_id                                                        int,                --
	unit_division														  varchar,
	unit_beat															  varchar,
	unit_type														      varchar,
	unit_shift															  varchar,
	unit_call_sign														  varchar,
	dispatch_type_code                                                    int,                --the reason for the dispatch
	dispatch_original_type												  varchar,
	dispatch_original_subtype											  varchar,
	dispatch_final_type													  varchar,
	dispatch_final_subtype												  varchar,
	response_plan_code                                                    int,                --result of the dispatch
	dispatch_original_priority_code                                       int,                --
	dispatch_final_priority_code                                          int,                --
	travel_time_minutes                                                   int,                --
	response_time_minutes                                                 int,                --
	time_on_scene_minutes                                                 int,                --
	datetime_assigned                                                     timestamp,          --
	datetime_arrived                                                      timestamp,          --
	datetime_cleared                                                      timestamp,           --
	sequence_assigned													  int,
	sequence_arrived													  int,
	units_assigned														  int,
	units_arrived														  int,
	dispatch_category 							varchar		-- officer/civilian/alarm iniciated
);

