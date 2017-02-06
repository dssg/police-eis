DROP TABLE IF EXISTS staging.police_area_demographics; 
CREATE  TABLE staging.police_area_demographics (
	police_area_id                                                        int,                --id for this particular shapefile: FK
	valid_start_date                                                      date,               --the date from which the demographic data is correct
	valid_end_date                                                        date,               --
	family_count_poverty_all_families                                     float,              --number of families and people whose income in the past 12 months is below the poverty level
	family_count_poverty_all_with_children_under_18                       float,              --number of families and people whose income in the past 12 months is below the poverty level
	family_count_poverty_all_with_children_under_5                        float,              --number of families and people whose income in the past 12 months is below the poverty level
	family_count_poverty_married_families                                 float,              --number of families and people whose income in the past 12 months is below the poverty level
	family_count_poverty_married_with_children_under_18                   float,              --number of families and people whose income in the past 12 months is below the poverty level
	family_count_poverty_married_with_children_under_5                    float,              --number of families and people whose income in the past 12 months is below the poverty level
	family_count_poverty_female_no_husband_families                       float,              --number of families and people whose income in the past 12 months is below the poverty level
	family_count_poverty_female_no_husband_with_children_under_18         float,              --number of families and people whose income in the past 12 months is below the poverty level
	family_count_poverty_female_no_husband_with_children_under_5          float,              --number of families and people whose income in the past 12 months is below the poverty level
	median_household_income                                               float,              --
	mean_household_income                                                 float,              --
	unemployment_rate                                                     float,              --Population 16 and over
	population_count                                                      float,              --
	race_count_black                                                      float,              --
	race_count_white                                                      float,              --
	race_count_native_american                                            float,              --
	race_count_asian                                                      float,              --
	race_count_other                                                      float,              --
	race_count_hispanic_latino                                            float,              --
	population_count_male                                                 float,              --
	population_count_female                                               float,              --
	population_under_5_years                                              float,              --
	population_5_to_9_years                                               float,              --
	population_10_to_14_years                                             float,              --
	population_15_to_19_years                                             float,              --
	population_20_to_24_years                                             float,              --
	population_25_to_34_years                                             float,              --
	population_35_to_44_years                                             float,              --
	population_45_to_54_years                                             float,              --
	population_55_to_59_years                                             float,              --
	population_60_to_64_years                                             float,              --
	population_65_to_74_years                                             float,              --
	population_75_to_84_years                                             float,              --
	population_85_and_over_years                                          float               --
);
