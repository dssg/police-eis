import pandas as pd
import numpy as np
import yaml
import pdb
from nose.tools import assert_equals
from sklearn import datasets
import copy

from eis import models, experiment 


with open('default.yaml', 'r') as f:
    config = yaml.load(f)

# Generate some dummy data
train_x, train_y = datasets.make_classification(n_samples=100, n_features=4)
test_x, test_y = datasets.make_classification(n_samples=100, n_features=4)

def try_model(config, model_name):
    test_config = config.copy() 
    test_config['model'] = [model_name]
        
    config_list = experiment.generate_models_to_run(test_config,
                                                        query_db=False)
    for exp in config_list:
        result_y, imp, modelobj, indiv_imps = models.run(train_x, train_y,
            test_x, exp.config['model'], exp.config['parameters'],
            n_cores=1)
        print('Experiment success!')
    return result_y


class TestModels:
    """Purpose of these tests is to check that the models
    and hyperparameter sets that will be generated in the experiments
    are valid.
    """

    def test_gen_model_random_forest(self):
        result_y = try_model(config, 'RandomForest')

        assert len(result_y) > 1

    def test_gen_model_logistic_regression(self):
        result_y = try_model(config, 'LogisticRegression')

        assert len(result_y) > 1

    def test_gen_model_extra_trees(self):
        result_y = try_model(config, 'ExtraTrees')

        assert len(result_y) > 1

    def test_gen_model_svm(self):
        result_y = try_model(config, 'SVM')

        assert len(result_y) > 1

    def test_gen_model_ada_boost(self):
        result_y = try_model(config, 'AdaBoost')

        assert len(result_y) > 1

    def test_gen_model_gradient_boosting(self):
        result_y = try_model(config, 'GradientBoostingClassifier')

        assert len(result_y) > 1

    def test_gen_model_decision_tree(self):
        result_y = try_model(config, 'DecisionTreeClassifier')

        assert len(result_y) > 1

    def test_gen_model_sgd_classifier(self):
        result_y = try_model(config, 'SGDClassifier')

        assert len(result_y) > 1

    def test_gen_model_knn(self):
        result_y = try_model(config, 'KNeighborsClassifier')

        assert len(result_y) > 1
