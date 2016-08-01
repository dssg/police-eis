DROP TABLE IF EXISTS staging.officer_children; 
CREATE UNLOGGED TABLE staging.officer_children (
	officer_id                                                            int references staging.officers_hub(officer_id) on delete cascade,                --officer id
	number_of_children                                                    int,                --whether the officer has children
	last_modified                                                         timestamp           --date at which marital/child status changed
);
