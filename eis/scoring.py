import sys
import pdb
import numpy as np
import pandas as pd
from sklearn import metrics
from . import dataset

def compute_AUC(test_labels, test_predictions):
    fpr, tpr, thresholds = metrics.roc_curve(
        test_labels, test_predictions, pos_label=1)
    return metrics.auc(fpr, tpr)


def precision_at_x_percent(test_labels, test_predictions, x_percent=0.01,
                           return_cutoff=False):

    cutoff_index = int(len(test_predictions) * x_percent)
    cutoff_index = min(cutoff_index, len(test_predictions) - 1)

    sorted_by_probability = np.sort(test_predictions)[::-1]
    cutoff_probability = sorted_by_probability[cutoff_index]

    test_predictions_binary = np.copy(test_predictions)
    test_predictions_binary[test_predictions_binary >= cutoff_probability] = 1
    test_predictions_binary[test_predictions_binary < cutoff_probability] = 0

    precision, _, _, _ = metrics.precision_recall_fscore_support(
        test_labels, test_predictions_binary)
    precision = precision[1]  # only interested in precision for label 1

    if return_cutoff:
        return precision, cutoff_probability
    else:
        return precision


def recall_at_x_percent(test_labels, test_predictions, x_percent=0.01,
                        return_cutoff=False):

    cutoff_index = int(len(test_predictions) * x_percent)
    cutoff_index = min(cutoff_index, len(test_predictions) - 1)

    sorted_by_probability = np.sort(test_predictions)[::-1]
    cutoff_probability = sorted_by_probability[cutoff_index]

    test_predictions_binary = np.copy(test_predictions)
    test_predictions_binary[test_predictions_binary >= cutoff_probability] = 1
    test_predictions_binary[test_predictions_binary < cutoff_probability] = 0

    _, recall, _, _ = metrics.precision_recall_fscore_support(
        test_labels, test_predictions_binary)
    recall = recall[1]  # only interested in precision for label 1

    if return_cutoff:
        return recall, cutoff_probability
    else:
        return recall

def get_test_predictions_binary(test_predictions, cutoff_probability=0.8):

    test_predictions_binary = np.copy(test_predictions)
    test_predictions_binary[test_predictions_binary >= cutoff_probability] = 1
    test_predictions_binary[test_predictions_binary < cutoff_probability] = 0

    return test_predictions_binary


#test_predictions_binary = get_test_predictions_binary(test_predictions)

def calculate_all_evaluation_metrics( test_label, test_predictions, test_predictions_binary, time_for_model_in_seconds ):
    """ Calculate several evaluation metrics using sklearn for a set of
        labels and predictions.
    :param list test_labels: list of true labels for the test data.
    :param list test_predictions: list of risk scores for the test data.
    :return: all_metrics
    :rtype: dict
    """

    all_metrics = dict()

    # compute built-in sklearn metrics.
    all_metrics["accuracy_score"] = metrics.accuracy_score( test_label, test_predictions_binary )
    all_metrics["auc_score"] = compute_AUC(test_label, test_predictions)
    all_metrics["roc_auc_score"]  = metrics.roc_auc_score( test_label, test_predictions )
    all_metrics["average_precision_score"] = metrics.average_precision_score( test_label, test_predictions )
    all_metrics["f1_score"] = metrics.f1_score( test_label, test_predictions_binary )
    all_metrics["fbeta_score_favor_precision"] = metrics.fbeta_score( test_label, test_predictions_binary, 0.75)
    all_metrics["fbeta_score_favor_recall"] = metrics.fbeta_score( test_label, test_predictions_binary, 1.25)
    all_metrics["precision_score_default"] = metrics.precision_score( test_label, test_predictions_binary )
    all_metrics["precision_score_at_top_point_01_percent"] = precision_at_x_percent(test_label, test_predictions, x_percent=0.01)
    all_metrics["precision_score_at_top_point_1_percent"] = precision_at_x_percent(test_label, test_predictions, x_percent=0.1)
    all_metrics["precision_score_at_top_1_percent"] = precision_at_x_percent(test_label, test_predictions, x_percent=1.0)
    all_metrics["precision_score_at_top_5_percent"] = precision_at_x_percent(test_label, test_predictions, x_percent=5.0)
    all_metrics["precision_score_at_top_10_percent"] = precision_at_x_percent(test_label, test_predictions, x_percent=10.0)
    all_metrics["recall_score_default"] = metrics.recall_score( test_label, test_predictions_binary )
    all_metrics["recall_score_at_top_point_01_percent"] = recall_at_x_percent(test_label, test_predictions, x_percent=0.01)
    all_metrics["recall_score_at_top_point_1_percent"] = recall_at_x_percent(test_label, test_predictions, x_percent=0.1)
    all_metrics["recall_score_at_top_1_percent"] = recall_at_x_percent(test_label, test_predictions, x_percent=1.0)
    all_metrics["recall_score_at_top_5_percent"] = recall_at_x_percent(test_label, test_predictions, x_percent=5.0)
    all_metrics["recall_score_at_top_10_percent"] = recall_at_x_percent(test_label, test_predictions, x_percent=10.0)
    all_metrics["time_for_model_in_seconds"] = time_for_model_in_seconds

    return all_metrics



def test_thresholds(testid, testprobs, start_date, end_date):
    """
    Compute confusion matrices for a range of thresholds for the DSaPP model
    """

    perc_thresholds = [x/100. for x in range(10, 85, 5)]
    confusion_matrices = {}
    for each_threshold in perc_thresholds:
        cm_eis, cm_dsapp = compute_confusion(testid, testprobs, each_threshold,
                                             start_date, end_date)
        confusion_matrices.update({each_threshold: {'eis': cm_eis,
                                                    'dsapp': cm_dsapp}})

    return confusion_matrices


def compute_confusion(testid, testprobs, at_x_perc, start_date, end_date):
    """
    Compute confusion matrices for baseline EIS performance during this time period
    as well as DSaPP model at x percent during this time period

    Args:
    testid - test ids for the DSaPP model
    testprobs - probabilities produced by the model
    at_x_perc - float between 0 and 1 defining the probability cutoff
    start_date - beginning of testing period
    end_date - end of testing period

    Returns:
    cm_eis - confusion matrix for the existing EIS during this time period
    cm_dsapp - confusion matrix for the DSaPP EIS during this time period
    and probability cutoff
    """

    # Score the existing EIS performance
    df = dataset.get_baseline(start_date, end_date)
    df['eisflag'] = 1

    # Score the DSaPP performance
    df_dsapp = assign_classes(testid, testprobs, at_x_perc)

    # Combine the old EIS and DSaPP EIS dataframes
    df_combined = df.merge(df_dsapp, on='officer_id', how='outer')
    df_combined['eisflag'] = df_combined['eisflag'].fillna(0)

    # Get whether these officers had adverse incidents or not
    df_adverse = dataset.get_labels_for_ids(df_combined['officer_id'], start_date, end_date)

    # Add the new column
    df_final = df_combined.merge(df_adverse, on='officer_id', how='outer').fillna(0)

    cm_eis = metrics.confusion_matrix(df_final['adverse_by_ourdef'].values, df_final['eisflag'].values)
    cm_dsapp = metrics.confusion_matrix(df_final['adverse_by_ourdef'].values, df_final['ourflag'].values)

    return cm_eis, cm_dsapp


def assign_classes(testid, predictions, x_percent):
    """
    Args:
    testid - list of ids
    predictions - list of probabilities
    x_percent - probability cutoff for positive and negative classes

    Returns:
    df - pandas DataFrame that contains test ids and integer class
    """
    cutoff_index = int(len(predictions) * x_percent)
    cutoff_index = min(cutoff_index, len(predictions) - 1)

    sorted_by_probability = np.sort(predictions)[::-1]
    cutoff_probability = sorted_by_probability[cutoff_index]

    predictions_binary = np.copy(predictions)
    predictions_binary[predictions_binary >= cutoff_probability] = 1
    predictions_binary[predictions_binary < cutoff_probability] = 0

    df = pd.DataFrame({'officer_id': testid, 'ourflag': predictions_binary})

    return df
