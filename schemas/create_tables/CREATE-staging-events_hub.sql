DROP TABLE IF EXISTS staging.events_hub CASCADE; 
CREATE UNLOGGED TABLE staging.events_hub (
	event_id                                                              serial primary key, -- unique id for the event
	event_chain_id                                                        int,				  -- id for the set of events that are linked together
	source_event_id 													  varchar,            -- unique id in the source table of this event. 
	department_defined_event_id											  varchar,            --    
	officer_id                                                            int references staging.officers_hub(officer_id),                --
	event_type_code                                                       int,                --
	event_datetime                                                        timestamp,          --
	shift_id                                                              int,                --
	dispatch_id															  varchar,
	off_duty_flag                                                         bool                --
);
