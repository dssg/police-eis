#!/usr/bin/env python
import logging
import pdb
import numpy as np

from sklearn import svm, preprocessing, ensemble, linear_model
from sklearn.feature_selection import SelectKBest
from treeinterpreter import treeinterpreter as ti


log = logging.getLogger(__name__)


class ConfigError():
    pass


def get_individual_importances(model, model_name, test_x):
    """
    Generate list of most important features for dashboard
    """
    if model_name == 'LogisticRegression': 
        coefficients = model.coef_[0]
        importances = np.copy(test_x)

        for person in range(test_x.shape[0]):
            one_invididual = test_x[person]
            single_importances = one_individual * coefficients
            importances[person] = single_importances[0]
        return importances

    elif model_name == 'RandomForest':
        prediction, bias, contributions = ti.predict(model, test_x) 
        pdb.set_trace()
        return contributions
    else:
        return None


def run(train_x, train_y, test_x, model, parameters):

    # Feature scaling
    scaler = preprocessing.StandardScaler().fit(train_x)
    train_x = scaler.transform(train_x)
    test_x = scaler.transform(test_x)

    results, importances, modelobj, individual_imp = gen_model(train_x, train_y, test_x, model,
                                     parameters)

    return results, importances, modelobj, individual_imp


def gen_model(train_x, train_y, test_x, model, parameters):
    log.info("Training {} with {}".format(model, parameters))
    modelobj = define_model(model, parameters)
    modelobj.fit(train_x, train_y)
    result_y = modelobj.predict_proba(test_x)

    individual_imp = get_individual_importances(modelobj, model, test_x)

    importances = get_feature_importances(modelobj)
    return result_y[:, 1], importances, modelobj, individual_imp


def get_feature_importances(model):
    try:
        return model.feature_importances_
    except:
        pass
    try:
        # Must be 1D for feature importance plot
        if len(model.coef_) <= 1:
            return model.coef_[0]
        else:
            return model.coef_
    except:
        pass
    return None


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
