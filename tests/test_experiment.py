from nose.tools import assert_equals


from eis import experiment


config = {}

class TestExperiment:
    def test_date_array(self):
        result = experiment.date_array(3600)
        assert result[0].year == 2007


    def test_generate_time_info_autogen_off(self):
        config["fake_today"] = ["01May2009", "01May2010"]
        config["training_window"] = [730, 1080]
        config["prediction_window"] = [180, 365]
        config["autogen_fake_todays"] = False
        
        expected_num_of_exps = len(config["fake_today"]) * \
                               len(config["training_window"]) * \
                               len(config["prediction_window"])
        temporal_info = experiment.generate_time_info(config)

        assert len(temporal_info) == expected_num_of_exps

 
    def test_generate_time_info_autogen_on(self):
        config["training_window"] = [730, 1080]
        config["prediction_window"] = [180, 365]
        config["autogen_fake_todays"] = True
        
        temporal_info = experiment.generate_time_info(config)

        assert len(temporal_info) == 58 
