#!/usr/bin/env python
import logging
import pdb
import numpy as np

from sklearn import (svm, ensemble, tree,
                     linear_model, neighbors, naive_bayes)
from sklearn.feature_selection import SelectKBest


log = logging.getLogger(__name__)


class ConfigError():
    pass


def get_individual_importances(model, model_name, test_x):
    """
    Generate list of most important features for dashboard
    """

    if model_name == 'LogisticRegression':
        coefficients = get_feature_importances(model)
        importances = np.copy(test_x)

        for person in range(test_x.shape[0]):
            one_individual = test_x[person]
            single_importances = one_individual * coefficients
            if len(single_importances) == 1:
                importances[person] = single_importances[0]
            else:
                importances[person] = single_importances
        return importances

    elif model_name == 'RandomForest':
        return None
    else:
        return None


def run(train_x, train_y, test_x, model, parameters, n_cores):

    predicted_y_probability, predicted_y_binary, modelobj = gen_model(train_x, train_y, test_x, model,
                                  parameters, n_cores=n_cores)

    # Model interpretability
    individual_imp = get_individual_importances(modelobj, model, test_x)
    importances = get_feature_importances(modelobj)

    return predicted_y_probability, predicted_y_binary, importances, modelobj, individual_imp


def gen_model(train_x, train_y, test_x, model, parameters, n_cores=1):
    """Trains a model and generates risk scores.

    Args:
        train_x: training features
        train_y: training target variable
        test_x: testing features
        model (str): model type
        parameters: hyperparameters for model
        n_cores (Optional[int]): number of cores to us

    Returns:
        result_y: predictions on test set
        modelobj: trained model object
    """

    log.info("Training {} with {}".format(model, parameters))
    modelobj = define_model(model, parameters, n_cores)
    modelobj.fit(train_x, train_y)
    result_y_probability = modelobj.predict_proba(test_x)
    result_y_binary = modelobj.predict(test_x)
    return result_y_probability[:, 1], result_y_binary, modelobj


def get_feature_importances(model):
    """
    Get feature importances (from scikit-learn) of trained model.

    Args:
        model: Trained model

    Returns:
        Feature importances, or failing that, None
    """

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


def define_model(model, parameters, n_cores):
    if model == "RandomForest":
        return ensemble.RandomForestClassifier(
            n_estimators=parameters['n_estimators'],
            max_features=parameters['max_features'],
            criterion=parameters['criterion'],
            max_depth=parameters['max_depth'],
            min_samples_split=parameters['min_samples_split'],
            n_jobs=n_cores)

    elif model == "RandomForestBagging":
        #TODO Make Model Bagging
        return ensemble.BaggingClassifier(
                    ensemble.RandomForestClassifier(
                        n_estimators=parameters['n_estimators'],
                        max_features=parameters['max_features'],
                        criterion=parameters['criterion'],
                        max_depth=parameters['max_depth'],
                        min_samples_split=parameters['min_samples_split'],
                        n_jobs=n_cores),
                    #Bagging parameters
                    n_estimators=parameters['n_estimators_bag'],
                    max_samples=parameters['max_samples'],
                    max_features=parameters['max_features_bag'],
                    bootstrap=parameters['bootstrap'],
                    bootstrap_features=parameters['bootstrap_features'],
                    n_jobs=n_cores
                    )

    elif model == "RandomForestBoosting":
        #TODO Make Model Boosting
        return ensemble.AdaBoostClassifier(
            ensemble.RandomForestClassifier(
                n_estimators=parameters['n_estimators'],
                max_features=parameters['max_features'],
                criterion=parameters['criterion'],
                max_depth=parameters['max_depth'],
                min_samples_split=parameters['min_samples_split'],
                n_jobs=n_cores),
            #Boosting parameters
            learning_rate=parameters['learning_rate'],
            algorithm=parameters['algorithm'],
            n_estimators=parameters['n_estimators_boost']
            )


    elif model == 'SVM':
        return svm.SVC(C=parameters['C_reg'],
                       kernel=parameters['kernel'],
                       probability=True)

    elif model == 'LogisticRegression':
        return linear_model.LogisticRegression(
            C=parameters['C_reg'],
            penalty=parameters['penalty'])

    elif model == 'AdaBoost':
        return ensemble.AdaBoostClassifier(
            learning_rate=parameters['learning_rate'],
            algorithm=parameters['algorithm'],
            n_estimators=parameters['n_estimators'])

    elif model == 'ExtraTrees':
        return ensemble.ExtraTreesClassifier(
            n_estimators=parameters['n_estimators'],
            max_features=parameters['max_features'],
            criterion=parameters['criterion'],
            max_depth=parameters['max_depth'],
            min_samples_split=parameters['min_samples_split'],
            n_jobs=n_cores)

    elif model == 'GradientBoostingClassifier':
        return ensemble.GradientBoostingClassifier(
            n_estimators=parameters['n_estimators'],
            learning_rate=parameters['learning_rate'],
            subsample=parameters['subsample'],
            max_depth=parameters['max_depth'])

    elif model == 'GaussianNB':
        return naive_bayes.GaussianNB()

    elif model == 'DecisionTreeClassifier':
        return tree.DecisionTreeClassifier(
            max_features=parameters['max_features'],
            criterion=parameters['criterion'],
            max_depth=parameters['max_depth'],
            min_samples_split=parameters['min_samples_split'])

    elif model == 'SGDClassifier':
        return linear_model.SGDClassifier(
            loss=parameters['loss'],
            penalty=parameters['penalty'],
            n_jobs=n_cores)

    elif model == 'KNeighborsClassifier':
        return neighbors.KNeighborsClassifier(
            n_neighbors=parameters['n_neighbors'],
            weights=parameters['weights'],
            algorithm=parameters['algorithm'],
            n_jobs=n_cores)

    else:
        raise ConfigError("Unsupported model {}".format(model))
