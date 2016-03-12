import pdb
import numpy as np
import pandas as pd
from sklearn import metrics

from eis import dataset


def compute_AUC(test_labels, test_predictions):
    fpr, tpr, thresholds = metrics.roc_curve(
        test_labels, test_predictions, pos_label=1)
    return metrics.auc(fpr, tpr)


def test_thresholds(testid, testprobs, start_date, end_date):
    """
    Compute confusion matrices for a range of thresholds for the DSaPP model
    """

    perc_thresholds = [x/100. for x in range(10, 70, 5)]
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
    df_combined = df.merge(df_dsapp, on='newid', how='outer')
    df_combined['eisflag'] = df_combined['eisflag'].fillna(0)

    # Get whether these officers had adverse incidents or not
    df_adverse = dataset.get_labels_for_ids(df_combined['newid'], start_date, end_date)

    # Add the new column
    df_final = df_combined.merge(df_adverse, on='newid', how='outer').fillna(0)

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

    df = pd.DataFrame({'newid': testid, 'ourflag': predictions_binary})

    return df