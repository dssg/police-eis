import sys
import pdb
import numpy as np
import pandas as pd
import statistics
from sklearn import metrics
from . import dataset


def compute_AUC(test_labels, test_predictions):
    fpr, tpr, thresholds = metrics.roc_curve(
        test_labels, test_predictions, pos_label=1)
    return metrics.auc(fpr, tpr)


def compute_avg_false_positive_rate(test_labels, test_predictions):
    fpr, tpr, thresholds = metrics.roc_curve(
        test_labels, test_predictions, pos_label=1)
    return statistics.mean(fpr)


def compute_avg_true_positive_rate(test_labels, test_predictions):
    fpr, tpr, thresholds = metrics.roc_curve(
        test_labels, test_predictions, pos_label=1)

    return statistics.mean(tpr)


def generate_binary_at_x(test_predictions, x_value, unit='abs'):
    if unit == 'pct':
        cutoff_index = int(len(test_predictions) * (x_value/ 100.00))
    else:
        cutoff_index = x_value
    test_predictions_binary = [1 if x < cutoff_index else 0 for x in range(len(test_predictions))]
    return test_predictions_binary


def precision_at_x(test_labels, test_prediction_binary_at_x):
    """Return the precision at a specified percent cutoff

    Args:
        test_labels: ground truth labels for the predicted data
        test_predictions: prediction labels
        x_proportion: the percent of the prediction population to label. Must be between 0 and 1.
    """
    precision, _, _, _ = metrics.precision_recall_fscore_support(
        test_labels, test_prediction_binary_at_x)
    precision = precision[1]  # only interested in precision for label 1

    return precision


def recall_at_x(test_labels, test_prediction_binary_at_x):
    _, recall, _, _ = metrics.precision_recall_fscore_support(
        test_labels,test_prediction_binary_at_x)
    recall = recall[1]  # only interested in precision for label 1

    return recall


def confusion_matrix_at_x(test_labels, test_prediction_binary_at_x):
    """
    Returns the raw number of a given metric:
        'TP' = true positives,
        'TN' = true negatives,
        'FP' = false positives,
        'FN' = false negatives

    """

    # compute true and false positives and negatives.
    true_positive = [1 if x == 1 and y == 1 else 0 for (x, y) in zip(test_prediction_binary_at_x, test_labels)]
    false_positive = [1 if x == 1 and y == 0 else 0 for (x, y) in zip(test_prediction_binary_at_x, test_labels)]
    true_negative = [1 if x == 0 and y == 0 else 0 for (x, y) in zip(test_prediction_binary_at_x, test_labels)]
    false_negative = [1 if x == 0 and y == 1 else 0 for (x, y) in zip(test_prediction_binary_at_x, test_labels)]

    TP = np.sum(true_positive)
    TN = np.sum(true_negative)
    FP = np.sum(false_positive)
    FN = np.sum(false_negative)

    return TP, TN, FP, FN


def calculate_all_evaluation_metrics( test_label, test_predictions, test_predictions_binary, time_for_model_in_seconds=None ):
    """ Calculate several evaluation metrics using sklearn for a set of
        labels and predictions.
    :param list test_labels: list of true labels for the test data.
    :param list test_predictions: list of risk scores for the test data.
    :return: all_metrics
    :rtype: dict
    """

    all_metrics = dict()

    # FORMAT FOR DICTIONARY KEY
    # all_metrics["metric|parameter|unit|comment"] OR
    # all_metrics["metric|parameter|unit"] OR
    # all_metrics["metric||comment"] OR
    # all_metrics["metric"]

    # Standard Metrics
    all_metrics["accuracy"] = metrics.accuracy_score(test_label, test_predictions_binary)
    all_metrics["auc|roc"] = metrics.roc_auc_score(test_label, test_predictions)
    all_metrics["average precision score"] = metrics.average_precision_score(test_label, test_predictions)
    all_metrics["f1"] = metrics.f1_score(test_label, test_predictions_binary)
    all_metrics["fbeta@|0.75 beta"] = metrics.fbeta_score(test_label, test_predictions_binary, 0.75)
    all_metrics["fbeta@|1.25 beta"] = metrics.fbeta_score(test_label, test_predictions_binary, 1.25)
    all_metrics["precision@|default"] = metrics.precision_score(test_label, test_predictions_binary)
    all_metrics["recall@|default"] = metrics.recall_score(test_label, test_predictions_binary)
    # all_metrics["time|seconds"] = time_for_model_in_seconds

    #sort
    test_predictions_sorted, test_label_sorted = zip(*sorted(zip(test_predictions, test_label),key=lambda pair: pair[0], reverse=True))

    # Threshold Metrics by Percentage
    parameters = {'pct': [0.01, 0.10, 0.25, 0.50, 1.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0],
               'abs': [10, 50, 100, 200, 500, 1000]}
    for x_type, x_values in parameters.items():
        for x_value in x_values:
            test_predictions_binary_at_x = generate_binary_at_x(test_predictions_sorted, x_value, unit= x_type)
            # Precision
            all_metrics["precision@|{}_{}".format(str(x_value), x_type)] = precision_at_x(test_label_sorted, test_predictions_binary_at_x)
            # Recall
            all_metrics["recall@|{}_{}".format(str(x_value), x_type)] = recall_at_x(test_label_sorted, test_predictions_binary_at_x)
            # Raw counts of officers we are flagging correctly and incorrectly at various fractions of the test set.
            TP, TN, FP, FN = confusion_matrix_at_x(test_label_sorted,  test_predictions_binary_at_x)
            all_metrics["true positives@|{}_{}".format(str(x_value), x_type)] = TP
            all_metrics["true negatives@|{}_{}".format(str(x_value), x_type)] = TN
            all_metrics["false positives@|{}_{}".format(str(x_value), x_type)] = FP
            all_metrics["false negatives@|{}_{}".format(str(x_value), x_type)] = FN
            
    return all_metrics

# Comment: Not used right now needs to be checked as it contains cut-off errors
# def test_thresholds(testid, testprobs, start_date, end_date):
#     """
#     Compute confusion matrices for a range of thresholds for the DSaPP model
#     """
#
#     perc_thresholds = [x/100. for x in range(10, 85, 5)]
#     confusion_matrices = {}
#     for each_threshold in perc_thresholds:
#         cm_eis, cm_dsapp = compute_confusion(testid, testprobs, each_threshold,
#                                              start_date, end_date)
#         confusion_matrices.update({each_threshold: {'eis': cm_eis,
#                                                     'dsapp': cm_dsapp}})
#
#     return confusion_matrices
#
#
# def compute_confusion(testid, testprobs, at_x_perc, start_date, end_date):
#     """
#     Compute confusion matrices for baseline EIS performance during this time period
#     as well as DSaPP model at x percent during this time period
#
#     Args:
#     testid - test ids for the DSaPP model
#     testprobs - probabilities produced by the model
#     at_x_perc - float between 0 and 1 defining the probability cutoff
#     start_date - beginning of testing period
#     end_date - end of testing period
#
#     Returns:
#     cm_eis - confusion matrix for the existing EIS during this time period
#     cm_dsapp - confusion matrix for the DSaPP EIS during this time period
#     and probability cutoff
#     """
#
#     # Score the existing EIS performance
#     df = dataset.get_baseline(start_date, end_date)
#     df['eisflag'] = 1
#
#     # Score the DSaPP performance
#     df_dsapp = assign_classes(testid, testprobs, at_x_perc)
#
#     # Combine the old EIS and DSaPP EIS dataframes
#     df_combined = df.merge(df_dsapp, on='officer_id', how='outer')
#     df_combined['eisflag'] = df_combined['eisflag'].fillna(0)
#
#     # Get whether these officers had adverse incidents or not
#     df_adverse = dataset.get_labels_for_ids(df_combined['officer_id'], start_date, end_date)
#
#     # Add the new column
#     df_final = df_combined.merge(df_adverse, on='officer_id', how='outer').fillna(0)
#
#     cm_eis = metrics.confusion_matrix(df_final['adverse_by_ourdef'].values, df_final['eisflag'].values)
#     cm_dsapp = metrics.confusion_matrix(df_final['adverse_by_ourdef'].values, df_final['ourflag'].values)
#
#     return cm_eis, cm_dsapp
#
#
# def assign_classes(testid, predictions, x_proportion):
#     """
#     Args:
#     testid - list of ids
#     predictions - list of probabilities
#     x_proportion - probability cutoff for positive and negative classes
#
#     Returns:
#     df - pandas DataFrame that contains test ids and integer class
#     """
#     cutoff_index = int(len(predictions) * x_proportion)
#     cutoff_index = min(cutoff_index, len(predictions) - 1)
#
#     sorted_by_probability = np.sort(predictions)[::-1]
#     cutoff_probability = sorted_by_probability[cutoff_index]
#     predictions_binary = [ 1 if x > cutoff_probability else 0 for x in test_predictions ]
#
#
#     df = pd.DataFrame({'officer_id': testid, 'ourflag': predictions_binary})
#
#     return df
