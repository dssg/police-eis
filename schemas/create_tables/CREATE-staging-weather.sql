DROP TABLE IF EXISTS staging.weather; 
CREATE  TABLE staging.weather (
	record_datetime                                                       timestamp,          --date and time of this weather record
	temperature_in_f                                                      float,              --temperature in degrees F
	sky_conditions_total_coverage_code                                    float,              --?
	liquid_precipitation_inches_one_hour                                  float,              --precipitation in the last one hour
	liquid_precipitation_inches_six_hours                                 float,              --precipitation in the last six hours
	wind_speed_mph                                                        float,              --wind speed measurement
	station_number                                                        int,                --
	latitude                                                              float,              --
	longitude                                                             float               --
);
