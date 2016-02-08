import pdb
import numpy as np
import pandas as pd
from sklearn import metrics


def compute_AUC(test_labels, test_predictions):
    fpr, tpr, thresholds = metrics.roc_curve(
        test_labels, test_predictions, pos_label=1)
    return metrics.auc(fpr, tpr)