import numpy as np
import pandas as pd
import logging
import sys
import pickle
import pdb
import datetime

from eis import dataset

log = logging.getLogger(__name__)

def compute_baseline(baseline, testid, testadverse):
    
    df_eis = pd.DataFrame(baseline)
    df_eis = df_eis.dropna()
    df_eis["eisflag"] = 1

    df_dsapp = pd.DataFrame({"newid": testid, "adverse": testadverse})

    ## What does the EIS flag?
    df = df_eis.merge(df_dsapp, how='left', on='newid')
    # can fill NaNs with 0 because those that have NaN for adverse or not
    # were not investigated
    df = df.fillna(0)  

    true_positives = len(df[df['adverse'] == 1])
    false_positives = len(df[df['adverse'] == 0])

    ## What does the EIS not flag?
    df2 = df_eis.merge(df_dsapp, how='right', on='newid')
    # can fill NaNs with 0 because those that have NaN for EIS were not flagged
    # by the old system
    df2 = df2.fillna(0) 
    all_unflagged = df2[df2['eisflag'] == 0]
    false_negatives = len(all_unflagged[all_unflagged['adverse'] == 1])
    true_negatives = len(all_unflagged[all_unflagged['adverse'] == 0])

    eis_baseline = {'tp': true_positives,
                    'fp': false_positives,
                    'fn': false_negatives,
                    'tn': true_negatives}

    return eis_baseline


def setup(config):
    fake_today = datetime.datetime.strptime(config["fake_today"], "%d%b%Y")
    train_start_date = datetime.datetime.strptime(config["fake_today"],
                                                  "%d%b%Y") - \
        datetime.timedelta(days=config["prediction_interval"])
    test_end_date = datetime.datetime.strptime(config["fake_today"],
                                               "%d%b%Y") + \
        datetime.timedelta(days=config["prediction_interval"])

    log.info("Train label window start: {}".format(train_start_date))
    log.info("Train label window stop: {}".format(fake_today))
    log.info("Test label window start: {}".format(fake_today))
    log.info("Test label window stop: {}".format(test_end_date))

    log.info("Loading officers and features to use as training...")
    train_x, train_y, train_id, names = dataset.grab_officer_data(
        config["features"], train_start_date, fake_today, train_start_date,
        config["accidents"], config["noinvest"])

    # Testing data should include ALL officers, ignoring "noinvest" keyword
    log.info("Loading officers and features to use as testing...")
    test_x, test_y, test_id, names = dataset.grab_officer_data(
        config["features"], fake_today, test_end_date, fake_today,
        config["accidents"], True)
    test_baseline = dataset.get_baseline(test_id, fake_today, test_end_date)
    eis_baseline = compute_baseline(test_baseline, test_id, test_y)

    return {"train_x": train_x,
            "train_y": train_y,
            "train_id": train_id,
            "test_x": test_x,
            "test_y": test_y,
            "test_id": test_id,
            "names": names,
            "train_start_date": train_start_date,
            "test_end_date": test_end_date,
            "eis_baseline": eis_baseline}