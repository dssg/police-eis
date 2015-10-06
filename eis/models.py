#!/usr/bin/env python
import logging
import pdb

from sklearn import svm, preprocessing, ensemble

log = logging.getLogger(__name__)


class ConfigError():
    pass

def run(train_x, train_y, test_x, config):

    # Feature scaling
    scaler = preprocessing.StandardScaler().fit(train_x)
    train_x = scaler.transform(train_x)
    test_x = scaler.transform(test_x)

    results = gen_model(train_x, train_y, test_x, config['model'],
                        config['parameters'])

    return results


def gen_model(train_x, train_y, test_x, model, parameters):
    log.info("Training {} with {}".format(model, parameters))
    model = define_model(model, parameters)
    model.fit(train_x, train_y)
    result_y = model.predict_proba(test_x)
    return result_y


def define_model(model, parameters):
    if model == "RandomForest":
        defaults = {"n_estimators": 10,
                    "depth": 10,
                    "max_features": "auto",
                    "criterion": "gini"}
        parameters = {name: parameters.get(name, defaults.get(name))
                      for name in defaults.keys()}
        return ensemble.RandomForestClassifier(
            n_estimators=parameters['n_estimators'],
            max_features=parameters['max_features'],
            criterion=parameters['criterion'],
            max_depth=parameters['depth'])

    elif model == 'SVM':
        return svm.SVC(C=parameters['C_reg'], kernel=parameters['kernel'])

    elif model == 'LogisticRegression':
        return linear_model.LogisticRegression(C=parameters['C_reg'])

    elif model == 'AdaBoost':
        return ensemble.AdaBoostClassifier(
            learning_rate=parameters['learning_rate'])

    else:
        raise ConfigError("Unsupported model {}".format(model))