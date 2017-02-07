import logging
import datetime
from sklearn import preprocessing
from dateutil.relativedelta import relativedelta
import pdb
from . import dataset
from .features import class_map

from . import setup_environment

log = logging.getLogger(__name__)


def run_traintest(config, as_of_dates_to_use):
    result = setup(config, as_of_dates_to_use)
    return result


def run_pilot(config):
    result = setup(config)
    return result


def setup(config, as_of_dates_to_use):
    """
    Sets up officer-level experiment

    Args:
    config: dict with config file
    as_of_dates_to_use: list of as_of_dates given the update window
    """

    train_start_date = datetime.datetime.strptime(config['train_start_date'], "%Y-%m-%d")
    train_end_date = datetime.datetime.strptime(config['train_end_date'], "%Y-%m-%d")
    test_end_date = datetime.datetime.strptime(config['test_end_date'], "%Y-%m-%d")
    test_start_date = datetime.datetime.strptime(config['test_start_date'], "%Y-%m-%d")

    log.info("Train start: {}".format(train_start_date))
    log.info("Train end date: {}".format(train_end_date))
    log.info("Test start date: {}".format(test_start_date))
    log.info("Test end datet: {}".format(test_end_date))

    log.info("Loading officers and features to use as training...")
    features_table = config["officer_feature_table_name"]
    train_x, train_y = dataset.get_dataset(
        train_start_date,
        train_end_date,
        config['prediction_window'],
        config['officer_past_activity_window'],
        config["officer_features"],
        config['officer_labels'],
        config['officer_feature_table_name'],
        config['officer_label_table_name'],
        as_of_dates_to_use)

    log.info("Loading officers and features to use as testing...")
    test_x, test_y = dataset.get_dataset(
        test_start_date,
        test_end_date,
        config['prediction_window'],
        config['officer_past_activity_window'],
        config["officer_features"],
        config['officer_labels'],
        config['officer_feature_table_name'],
        config['officer_label_table_name'],
        [test_end_date])

    # Testing data should include ALL officers, ignoring "noinvest" keyword
    testing_labelling_config = config["officer_labels"].copy()
    # testing_labelling_config["noinvest"] = True

    train_x_index = train_x.index
    test_x_index = test_x.index
    features = train_x.columns.values

    # Feature scaling
    # scaler = preprocessing.StandardScaler().fit(train_x)
    # train_x = scaler.transform(train_x)
    # test_x = scaler.transform(test_x)
    train_x = train_x.as_matrix()
    test_x = test_x.as_matrix()

    return {"train_x": train_x,
            "train_y": train_y,
            "train_id": train_x_index,
            "test_x": test_x,
            "test_y": test_y,  # For pilot test_y will not be used
            "test_id": test_x_index,
            "names": features,
            "train_start_date": train_start_date,
            "train_end_date": train_end_date,
            "officer_past_activity_window": config["officer_past_activity_window"],
            "test_start_date": test_start_date,
            "test_end_date": test_end_date,
            "train_x_index": train_x_index,
            "test_x_index": test_x_index,
            "features": features}


def get_officer_features_table_columns(config):
    """ Creates temporary instances of feature classes to get a list of all feature table column names """

    # get a list of all features that are set to true.
    feature_names = config["officer_features"]
    feature_blocks = config["feature_blocks"]
    officer_feature_table = config["officer_feature_table_name"]

    active_features = []
    for block in feature_names:
        active_features += [key for key in feature_blocks[block] if feature_blocks[block][key] == True]

    selected_time_window = config["temporal_info"]["timegated_feature_lookback_duration"]
    engine = setup_environment.get_database()
    # connect to the database and get the feature list (TODO: switch to factory pattern for db connection)
    query = """
            WITH full_list AS (
              /* get all columns from the specified feature table*/
                SELECT column_name AS column_name
                FROM information_schema.columns
            WHERE table_schema = '{schema}'
                  AND table_name = '{feature_table}'
            ), list_cut AS (
              /* seperate the full name into parts, e.g IR_officer_id_1d_IncidentsSeverityUnknown_major_sum ->
              * 1d,IncidentsSeverityUnknown */
                SELECT
                  regexp_matches(column_name, $$_id_(\d+\w|all)?[_]?([A-Z][A-Za-z0-9]+)_$$) AS array_col,
                  column_name
                FROM full_list
                WHERE column_name LIKE $$%%_id_%%$$
            )
              , db_avaliable_features AS (
              /* convert to string for matching: e.g 1d_IncidentsSeverityUnknown  */
                SELECT
                  CASE WHEN array_col [1] NOTNULL
                    THEN array_col [1] :: TEXT || '_' || array_col [2] :: TEXT
                  ELSE array_col [2] :: TEXT END AS db_created_features,
                  column_name,
                  array_col
                FROM list_cut
            ), selected_columns AS (
                SELECT unnest(
                    ARRAY{requested_features}) --insert of the requested features
            ), selected_timewindow AS (
                SELECT unnest(ARRAY{requested_time_window}) --insert of the requested time window
            ), non_timewindow_colums AS ( -- get all possible features without time-windows in the database table
                SELECT db_created_features
                FROM db_avaliable_features
                WHERE array_col [1] ISNULL
                GROUP BY db_created_features
            ), all_colums AS ( -- get all possible features with time span all in the name in the database table
                SELECT replace(db_created_features, 'all_', '') AS db_created_features
                FROM db_avaliable_features
                WHERE array_col [1] = 'all'
                GROUP BY db_created_features
            ), requested_features AS (
              SELECT unnest(t) || '_' || unnest(f) AS r_columns
              FROM selected_timewindow t CROSS JOIN selected_columns f
              EXCEPT -- exclude all features from the cross product that do not have a time-window
              SELECT unnest(t) || '_' || f.db_created_features AS r_columns
              FROM selected_timewindow t CROSS JOIN non_timewindow_colums f
              EXCEPT -- exclude all features from the cross product that do have the window all
              SELECT unnest(t) || '_' || f.db_created_features AS r_columns
              FROM selected_timewindow t CROSS JOIN all_colums f
              UNION (-- add all the single features that are requested which  did not have a time-window in the database
                SELECT db_created_features AS r_columns
                FROM non_timewindow_colums
                INTERSECT
                SELECT unnest(f) AS r_columns
                FROM selected_columns f
              )
              UNION (-- add all the single features that are requested which  have all as timewindow in the database
                SELECT ('all_' || r_columns) AS r_columns
                FROM (
                       SELECT db_created_features AS r_columns
                       FROM all_colums
                       INTERSECT
                       SELECT unnest(f) AS r_columns
                       FROM selected_columns f
                     ) s
              )
            ), final_avaliable_colums AS (
                SELECT array_agg(column_name :: TEXT
                       ORDER BY 1) AS col_avaliable
                FROM requested_features r
                  JOIN db_avaliable_features a ON r.r_columns = a.db_created_features
            ), final_missing_columns AS (
                SELECT array_agg(r_columns
                       ORDER BY 1) AS col_missing
                FROM requested_features r
                  LEFT JOIN db_avaliable_features a ON r.r_columns = a.db_created_features
                WHERE a.column_name ISNULL
            )
            SELECT
              (SELECT *
               FROM final_avaliable_colums),
              (SELECT *
               FROM final_missing_columns);
            """.format(
        schema='features',
        feature_table=officer_feature_table,
        requested_features=active_features,
        requested_time_window=selected_time_window
    )

    result = engine.connect().execute(query)

    # returns 2 dicts, 'col_avaliable' and 'col_missing'
    resultset = [dict(row) for row in result]
    result_dict = resultset[0]
    log.error('These features are missing: {}'.format(result_dict['col_missing']))

    return result_dict['col_avaliable']
