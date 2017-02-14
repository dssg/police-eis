DROP TABLE IF EXISTS staging.landmarks; 
CREATE  TABLE staging.landmarks (
	landmark_id                                                           int,                --unique identifier of this landmark
	landmark_type_code                                                    int,                --landmark type, integer keyed to lookup_landmark_type_codes
	latitude                                                              float,              --
	longitude                                                             float,              --
	address_id                                                            int                 --PK in addresses table
);
