import pandas as pd
from eis import setup_environment
import json
import yaml
import sys

"""
Example: To run with model_id 19932 
python generate_feature_mapping.py 19932
Output: 19932_feature_mapping.csv
"""
## Setup connection
engine = setup_environment.get_connection_from_profile(config_file_name="default_profile.yaml")

model_id = sys.argv[1]

## Load the features_config
with open('eis/features/features_descriptions.yaml') as f:
	features_config = yaml.load(f)

## Get the feature names you are trying to map
feature_query = "SELECT feature from results.feature_importances where model_id = {}".format(model_id)
feature_names = pd.read_sql(feature_query, engine)

## Build queries 
def get_query(feature_name):
	query = """SELECT * from public.get_feature_complete_description('{feature}',
			'{feature_names}'::JSON, '{time_aggregations}'::JSON, '{metrics}'::JSON)""".format(feature=feature_name,feature_names=json.dumps(features_config['feature_names']), time_aggregations = json.dumps(features_config['time_aggregations']), metrics = json.dumps(features_config['metrics_name']))
	return query

list_of_dfs = []
for i in range(len(feature_names)):
	list_of_dfs.append(pd.read_sql(get_query(feature_names.feature[i]), engine))

## Concat the dfs into one df
feature_mapping = pd.concat(list_of_dfs, axis=0, ignore_index=True)

## Write to csv
feature_mapping.to_csv(str(model_id)+'_feature_mapping.csv', index=False, quotechar='|')

			


