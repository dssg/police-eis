import numpy as np
import pdb
import pandas as pd
import yaml
import logging
import sys
import datetime

from eis import setup_environment
from eis.features import officers as featoff


log = logging.getLogger(__name__)


def imputation_zero(df, ids):
    fulldf = pd.DataFrame(ids, columns=["newid"] + [df.columns[0]]) 
    df["newid"] = df.index
    newdf = df.merge(fulldf, how="right", on="newid")
    newdf = newdf.fillna(0)
    newdf[df.columns[0]] = newdf[df.columns[0] + "_x"] + newdf[df.columns[0] + "_y"] 
    newdf = newdf.drop([df.columns[0] + "_x", df.columns[0] + "_y"], axis=1)
    newdf = newdf.set_index("newid")
    return newdf


def imputation_mean(df):
    try:
        newdf = df.set_index("newid")
    except:
        newdf = df
    for i in range(len(newdf.columns)):
        this_col = newdf.columns[i]
        newdf[this_col] = newdf[this_col].fillna(np.mean(newdf[this_col]))
    return newdf


def convert_series(df):
    onecol = df.columns[0]
    numcols = len(df[onecol].iloc[0])
    newcols = [onecol + '_' + str(i) for i in range(numcols)]

    newdf = pd.DataFrame(columns=newcols, index=df.index)
    for i in range(len(df)):
        newdf.iloc[i] = df[onecol].iloc[i]

    return newdf.astype(int), newcols


def convert_categorical(df):
    """
    this function generates features from a nominal feature

    Inputs:
    df: a dataframe with two columns: id, and a feature which is 1
    of n categories

    Outputs:
    df: a dataframe with 1 + n columns: id and boolean features that are
    "category n" or not
    """

    onecol = df.columns[0]
    categories = pd.unique(df[onecol])

    # Remove empty fields
    # Replace Nones or empty fields with NaNs?
    categories = [x for x in categories if x is not None]
    try:
        categories.remove(' ')
    except:
        pass

    # Get rid of capitalization differences
    categories = list(set([str.lower(x) for x in categories]))

    # Set up new features
    featnames = []
    if len(categories) == 2:
        newfeatstr = 'is_' + categories[0]
        featnames.append(newfeatstr)
        df[newfeatstr] = df[onecol] == categories[0]
    else:
        for i in range(len(categories)):
            if type(categories[i]) is str:
                newfeatstr = 'is_' + categories[i]
                featnames.append(newfeatstr)
                df[newfeatstr] = df[onecol] == categories[i]

    df = df.drop(onecol, axis=1)
    return df.astype(int), list(df.columns)


def lookup(feature, **kwargs):

    if feature[1:3] == "yr":
        kwargs["feat_time_window"] = int(feature[0])
    else:
        kwargs["feat_time_window"] = 15

    dict_lookup = {'height_weight': featoff.OfficerHeightWeight(**kwargs),
                    'education': featoff.OfficerEducation(**kwargs),
                    '1yrprioralladverse': featoff.CountPriorAdverse(**kwargs),
                    'careerprioralladverse': featoff.CountPriorAdverse(**kwargs),
                    '1yrprioraccident': featoff.CountPriorAccident(**kwargs),
                    'careerprioraccident': featoff.CountPriorAccident(**kwargs),
                    '1yrnumfilteredadverse': featoff.CountPriorFilteredAdverse(**kwargs),
                    'careernumfilteredadverse': featoff.CountPriorFilteredAdverse(**kwargs),
                    '1yrroccoc': featoff.CountRocCOC(**kwargs),
                    'careerroccoc': featoff.CountRocCOC(**kwargs),
                    '1yrrocia': featoff.CountRocIA(**kwargs),
                    'careerrocia': featoff.CountRocIA(**kwargs),
                    '1yrpreventable': featoff.CountPreventable(**kwargs),
                    'careerpreventable': featoff.CountPreventable(**kwargs),
                    '1yrunjustified': featoff.CountUnjustified(**kwargs),
                    'careerunjustified': featoff.CountUnjustified(**kwargs),
                    '1yrsustaincompl': featoff.CountSustainedComplaints(**kwargs),
                    'careersustaincompl': featoff.CountSustainedComplaints(**kwargs),
                    '1yriaconcerns': featoff.IAConcerns(**kwargs),
                    'careeriaconcerns': featoff.IAConcerns(**kwargs),
                    'careeriarate': featoff.IARate(**kwargs),
                    '1yrdofcounts': featoff.DOFTypeCounts(**kwargs),
                    'careerdofcounts': featoff.DOFTypeCounts(**kwargs),
                    '1yrdirectivecounts': featoff.DirectiveViolCounts(**kwargs),
                    'careerdirectivecounts': featoff.DirectiveViolCounts(**kwargs),
                    '1yriaeventtypes': featoff.IAEventTypeCounts(**kwargs),
                    'careeriaeventtypes': featoff.IAEventTypeCounts(**kwargs),
                    '1yrinterventions': featoff.SuspensionCounselingTime(**kwargs),
                    'careerinterventions': featoff.SuspensionCounselingTime(**kwargs),
                    '1yrweaponsuse': featoff.NormalizedCountsWeaponsUse(**kwargs),
                    'careerweaponsuse': featoff.NormalizedCountsWeaponsUse(**kwargs),
                    '1yrunithistory': featoff.CountUnit(**kwargs),
                    'careerunithistory': featoff.CountUnit(**kwargs),
                    '1yrdivisionhistory': featoff.CountDivision(**kwargs),
                    'careerdivisionhistory': featoff.CountDivision(**kwargs),
                    'yearsexperience': featoff.OfficerYrsExperience(**kwargs),
                    'daysexperience': featoff.OfficerDaysExperience(**kwargs),
                    'malefemale': featoff.OfficerMaleFemale(**kwargs),
                    'race': featoff.OfficerRace(**kwargs),
                    'officerage': featoff.OfficerAge(**kwargs),
                    'officerageathire': featoff.OfficerAgeAtHire(**kwargs),
                    'maritalstatus': featoff.OfficerMaritalStatus(**kwargs),
                    'careerarrests': featoff.OfficerCareerArrests(**kwargs),
                    'numrecentarrests': featoff.NumRecentArrests(**kwargs),
                    'careerNPCarrests': featoff.NPCArrests(**kwargs),
                    '1yrNPCarrests': featoff.NPCArrests(**kwargs),
                    'careerdiscarrests': featoff.DiscArrests(**kwargs),
                    '1yrdiscarrests': featoff.DiscArrests(**kwargs),
                    'arresttod': featoff.OfficerAvgTimeOfDayArrests(**kwargs),
                    'arresteeage': featoff.OfficerAvgAgeArrests(**kwargs),
                    'disconlyarrests': featoff.DiscOnlyArrestsCount(**kwargs),
                    'arrestratedelta': featoff.ArrestRateDelta(**kwargs),
                    'arresttimeseries': featoff.OfficerArrestTimeSeries(**kwargs),
                    'arrestcentroids': featoff.ArrestCentroids(**kwargs),
                    'careernpccitations': featoff.NPCCitations(**kwargs),
                    '1yrnpccitations': featoff.NPCCitations(**kwargs),
                    'careercitations': featoff.Citations(**kwargs),
                    '1yrcitations': featoff.Citations(**kwargs),
                    'numsuicides': featoff.YearNumSuicides(**kwargs),
                    'numjuveniles': featoff.YearNumJuvenileVictim(**kwargs),
                    'numdomesticviolence': featoff.YearNumDomesticViolence(**kwargs),
                    'numhate': featoff.YearNumHate(**kwargs),
                    'numnarcotics': featoff.YearNumNarcotics(**kwargs),
                    'numgang': featoff.YearNumGang(**kwargs),
                    'numgunknife': featoff.YearNumGunKnife(**kwargs),
                    'numpersweaps': featoff.YearNumPersWeaps(**kwargs),
                    'avgagevictims': featoff.AvgAgeVictims(**kwargs),
                    'minagevictims': featoff.MinAgeVictims(**kwargs),
                    'careerficount': featoff.CareerFICount(**kwargs),
                    'recentficount': featoff.RecentFICount(**kwargs),
                    'careernontrafficficount': featoff.CareerNonTrafficFICount(**kwargs),
                    'recentnontrafficficount': featoff.RecentNonTrafficFICount(**kwargs),
                    'careerhighcrimefi': featoff.CareerHighCrimeAreaFI(**kwargs),
                    'recenthighcrimefi': featoff.RecentHighCrimeAreaFI(**kwargs),
                    '1yrloiterfi': featoff.LoiterFI(**kwargs),
                    'careerloiterfi': featoff.LoiterFI(**kwargs),
                    'careerblackfi': featoff.CareerBlackFI(**kwargs),
                    'careerwhitefi': featoff.CareerWhiteFI(**kwargs),
                    'avgsuspectagefi': featoff.FIAvgSuspectAge(**kwargs),
                    'avgtimeofdayfi': featoff.FIAvgTimeOfDay(**kwargs),
                    'fitimeseries': featoff.FITimeseries(**kwargs),
                    'careercadstats': featoff.CADStatistics(**kwargs),
                    '1yrcadstats': featoff.CADStatistics(**kwargs),
                    'careerelectivetrain': featoff.CareerElectHoursTrain(**kwargs),
                    'recentelectivetrain': featoff.RecentElectHoursTrain(**kwargs),
                    'careerhourstrain': featoff.CareerHoursTrain(**kwargs),
                    'recenthourstrain': featoff.RecentHoursTrain(**kwargs),
                    'careerworkouthours': featoff.CareerHoursPhysFit(**kwargs),
                    'recentworkouthours': featoff.RecentHoursPhysFit(**kwargs),
                    'careerrochours': featoff.CareerHoursROCTrain(**kwargs),
                    'recentrochours': featoff.RecentHoursROCTrain(**kwargs),
                    'careerproftrain': featoff.CareerHoursProfTrain(**kwargs),
                    'recentproftrain': featoff.RecentHoursProfTrain(**kwargs),
                    'careertrafficstopnum': featoff.CareerNumTrafficStops(**kwargs),
                    'recenttrafficstopnum': featoff.RecentNumTrafficStops(**kwargs),
                    'careerdomvioltrain': featoff.CareerHoursDomViolTrain(**kwargs),
                    'recentdomvioltrain': featoff.RecentHoursDomViolTrain(**kwargs),
                    'careermilitarytrain': featoff.CareerHoursMilitaryReturn(**kwargs),
                    'recentmilitarytrain': featoff.RecentHoursMilitaryReturn(**kwargs),
                    'careertasertrain': featoff.CareerHoursTaserTrain(**kwargs),
                    'recenttasertrain': featoff.RecentHoursTaserTrain(**kwargs),
                    'careerbiastrain': featoff.CareerHoursBiasTrain(**kwargs),
                    'recentbiastrain': featoff.RecentHoursBiasTrain(**kwargs),
                    'careerforcetrain': featoff.CareerHoursForceTrain(**kwargs),
                    'recentforcetrain': featoff.RecentHoursForceTrain(**kwargs),
                    'careertsuofarr': featoff.CareerNumTStopRunTagUOFOrArrest(**kwargs),
                    'recenttsuofarr': featoff.RecentNumTStopRunTagUOFOrArrest(**kwargs),
                    'careerforcetraffic': featoff.CareerNumTrafficStopsForce(**kwargs),
                    'recentforcetraffic': featoff.RecentNumTrafficStopsForce(**kwargs),
                    'careertsblackdaynight': featoff.CareerTSPercBlackDayNight(**kwargs),
                    'recenttsblackdaynight': featoff.RecentTSPercBlackDayNight(**kwargs),
                    '1yrtrafstopresist': featoff.NumTrafficStopsResist(**kwargs),
                    '3yrtrafstopresist': featoff.NumTrafficStopsResist(**kwargs),
                    '5yrtrafstopresist': featoff.NumTrafficStopsResist(**kwargs),
                    'careertrafstopresist': featoff.NumTrafficStopsResist(**kwargs),
                    '1yrtrafstopsearch': featoff.TrafficStopsSearch(**kwargs),
                    '3yrtrafstopsearch': featoff.TrafficStopsSearch(**kwargs),
                    '5yrtrafstopsearch': featoff.TrafficStopsSearch(**kwargs),
                    'careertrafstopsearch': featoff.TrafficStopsSearch(**kwargs),
                    '1yrtrafstopsearchreason': featoff.TrafficStopSearchReason(**kwargs),
                    '3yrtrafstopsearchreason': featoff.TrafficStopSearchReason(**kwargs),
                    '5yrtrafstopsearchreason': featoff.TrafficStopSearchReason(**kwargs),
                    'careertrafstopsearchreason': featoff.TrafficStopSearchReason(**kwargs),
                    '1yrtrafstopruntagreason': featoff.TrafficStopRunTagReason(**kwargs),
                    '3yrtrafstopruntagreason': featoff.TrafficStopRunTagReason(**kwargs),
                    '5yrtrafstopruntagreason': featoff.TrafficStopRunTagReason(**kwargs),
                    'careertrafstopruntagreason': featoff.TrafficStopRunTagReason(**kwargs),
                    '1yrtrafstopresult': featoff.TrafficStopResult(**kwargs),
                    '3yrtrafstopresult': featoff.TrafficStopResult(**kwargs),
                    '5yrtrafstopresult': featoff.TrafficStopResult(**kwargs),
                    'careertrafstopresult': featoff.TrafficStopResult(**kwargs),
                    '1yrtrafstopbyrace': featoff.TrafficStopFracRace(**kwargs),
                    '3yrtrafstopbyrace': featoff.TrafficStopFracRace(**kwargs),
                    '5yrtrafstopbyrace': featoff.TrafficStopFracRace(**kwargs),
                    'careertrafstopbyrace': featoff.TrafficStopFracRace(**kwargs),
                    '1yrtrafstopbygender': featoff.TrafficStopFracGender(**kwargs),
                    '3yrtrafstopbygender': featoff.TrafficStopFracGender(**kwargs),
                    '5yrtrafstopbygender': featoff.TrafficStopFracGender(**kwargs),
                    'careertrafstopbygender': featoff.TrafficStopFracGender(**kwargs),
                    'trafficstoptimeseries': featoff.TrafficStopTimeSeries(**kwargs),
                    '1yreiswarnings': featoff.EISWarningsCount(**kwargs),
                    '5yreiswarnings': featoff.EISWarningsCount(**kwargs),
                    'careereiswarnings': featoff.EISWarningsCount(**kwargs),
                    '1yreiswarningtypes': featoff.EISWarningByTypeFrac(**kwargs),
                    'careereiswarningtypes': featoff.EISWarningByTypeFrac(**kwargs),
                    '1yreiswarninginterventions': featoff.EISWarningInterventionFrac(**kwargs),
                    'careereiswarninginterventions': featoff.EISWarningInterventionFrac(**kwargs),
                    '1yrextradutyhours': featoff.ExtraDutyHours(**kwargs),
                    'careerextradutyhours': featoff.ExtraDutyHours(**kwargs),
                    '1yrextradutyneighb1': featoff.ExtraDutyNeighborhoodFeatures1(**kwargs),
                    'careerextradutyneighb1': featoff.ExtraDutyNeighborhoodFeatures1(**kwargs),
                    '1yrextradutyneighb2': featoff.ExtraDutyNeighborhoodFeatures2(**kwargs),
                    'careerextradutyneighb2': featoff.ExtraDutyNeighborhoodFeatures2(**kwargs),
                    '1yrneighb1': featoff.AvgNeighborhoodFeatures1(**kwargs),
                    'careerneighb1': featoff.AvgNeighborhoodFeatures1(**kwargs),
                    '1yrneighb2': featoff.AvgNeighborhoodFeatures2(**kwargs),
                    'careerneighb2': featoff.AvgNeighborhoodFeatures2(**kwargs)}


    if feature not in dict_lookup.keys():
        raise UnknownFeatureError(feature)

    return dict_lookup[feature]


class UnknownFeatureError(Exception):
    def __init__(self, feature):
        self.feature = feature

    def __str__(self):
        return "Unknown feature: {}".format(self.feature)


class FeatureLoader():

    def __init__(self, start_date, end_date, fake_today):

        engine, config = setup_environment.get_database()

        self.con = engine.raw_connection()
        self.con.cursor().execute("SET SCHEMA '{}'".format(config['schema']))
        self.start_date = start_date
        self.end_date = end_date
        self.fake_today = fake_today
        self.tables = config  # Dict of tables
        self.schema = config['schema']

    def officer_labeller(self, accidents, noinvest):
        """
        Load the IDs for a set of officers investigated between
        two dates and the outcomes

        Inputs:
        accidents: Bool representing if accidents should be included
        noinvest: Bool representing how officers with no investigations 
        should be treated - True means they are included as "0", False means they
        are excluded

        Returns:
        labels: pandas dataframe with two columns:
        newid and adverse_by_ourdef
        """

        log.info("Loading labels...")

        if noinvest == True:
            qinvest = "SELECT newid from {}".format(self.tables["active_officers"])
        else:
            qinvest = ("SELECT newid, count(adverse_by_ourdef) from {} "
                      "WHERE dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by newid "
                      ).format(self.tables["si_table"],
                               self.start_date, self.end_date)

        if accidents == False:
            qadverse = ("SELECT newid, count(adverse_by_ourdef) from {} "
                        "WHERE adverse_by_ourdef = 1 "
                        "AND eventtype != 'Accident' "
                        "AND dateoccured >= '{}'::date "
                        "AND dateoccured <= '{}'::date "
                        "group by newid "
                        ).format(self.tables["si_table"],
                                 self.start_date, self.end_date)
        else:     
            qadverse = ("SELECT newid, count(adverse_by_ourdef) from {} "
                        "WHERE adverse_by_ourdef = 1 "
                        "AND dateoccured >= '{}'::date "
                        "AND dateoccured <= '{}'::date "
                        "group by newid "
                        ).format(self.tables["si_table"],
                                 self.start_date, self.end_date)

        invest = pd.read_sql(qinvest, con=self.con)
        adverse = pd.read_sql(qadverse, con=self.con)
        adverse["adverse_by_ourdef"] = 1
        adverse = adverse.drop(["count"], axis=1)
        if noinvest == False:
            invest = invest.drop(["count"], axis=1)
        outcomes = adverse.merge(invest, how='right', on='newid')
        outcomes = outcomes.fillna(0)

        # labels = labels.set_index(["newid"])

        # should be no duplicates

        return outcomes

    def dispatch_labeller(self):
        """
        Load the dispatch events investigated between
        two dates and the outcomes

        Returns:
        labels: pandas dataframe with two columns:
        newid, adverse_by_ourdef, dateoccured
        """

        log.info("Loading labels...")

        # These are the objects flagged as ones and twos
        query = ("SELECT newid, adverse_by_ourdef, "
                 "dateoccured from {}.{} "
                 "WHERE dateoccured >= '{}'::date "
                 "AND dateoccured <= '{}'::date"
                 ).format(self.schema, self.tables["si_table"],
                          self.start_date, self.end_date)

        labels = pd.read_sql(query, con=self.con)

        # Now also label those not sampled
        # Grab all dispatch events and filter out the ones
        # already included

        pdb.set_trace()

        return labels

    def loader(self, features_to_load, ids):
        kwargs = {"time_bound": self.fake_today,
                  "feat_time_window": 0}
        feature = lookup(features_to_load, **kwargs)

        if type(feature.query) == str:
            results = self.__read_feature_from_db(feature.query,
                                                  features_to_load,
                                                  drop_duplicates=True)
            featurenames = feature.name_of_features
        elif type(feature.query) == list:
            featurenames = []
            results = pd.DataFrame()
            for each_query in feature.query:
                ea_resu = self.__read_feature_from_db(each_query,
                                                      features_to_load,
                                                      drop_duplicates=True)
                featurenames = featurenames + feature.name_of_features

                if len(results) == 0:
                    results = ea_resu
                    results['newid'] = ea_resu.index
                else:   
                    results = results.join(ea_resu, how='left', on='newid')

        if feature.type_of_features == "categorical":
            results, featurenames = convert_categorical(results)
        elif feature.type_of_features == "series":
            results, featurenames = convert_series(results)

        if feature.type_of_imputation == "zero":
                results = imputation_zero(results, ids)
        elif feature.type_of_imputation == "mean":
                results = imputation_mean(results)

        return results, featurenames

    def __read_feature_from_db(self, query, features_to_load,
                               drop_duplicates=True):

        log.debug("Loading features for events from "
                  "%(start_date)s to %(end_date)s".format(
                        self.start_date, self.end_date))

        results = pd.read_sql(query, con=self.con)

        if drop_duplicates:
            results = results.drop_duplicates(subset=["newid"])

        results = results.set_index(["newid"])
        # features = features[features_to_load]

        log.debug("... {} rows, {} features".format(len(results),
                                                    len(results.columns)))
        return results


def grab_officer_data(features, start_date, end_date, time_bound):
    """
    Function that defines the dataset.

    Inputs:
    -------
    features: dict containing which features to use
    start_date: start date for selecting officers
    end_date: end date for selecting officers
    time_bound: build features with respect to this date
    """

    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    data = FeatureLoader(start_date, end_date, time_bound)

    officers = data.officer_labeller(accidents=False, noinvest=True)
    # officers.set_index(["newid"])

    dataset = officers
    featnames = []
    for each_feat in features:
        if features[each_feat] == True:
            feature_df, names = data.loader(each_feat,
                                            dataset["newid"])
            log.info("Loaded feature {} with {} rows".format(
                featnames, len(feature_df)))
            featnames = featnames + names
            dataset = dataset.join(feature_df, how='left', on='newid')

    dataset = dataset.reset_index()
    dataset = dataset.reindex(np.random.permutation(dataset.index))
    dataset = dataset.set_index(["newid"])

    dataset = dataset.dropna()

    labels = dataset["adverse_by_ourdef"].values
    feats = dataset.drop(["adverse_by_ourdef", "index"], axis=1)
    ids = dataset.index.values

    # Imputation will go here

    log.debug("Dataset has {} rows and {} features".format(
       len(labels), len(feats.columns)))

    return feats, labels, ids, featnames
