import json
import sqlalchemy

CONFIG_PATH = "~/default_profile.json"
with open(CONFIG_PATH) as f:
    config = json.load(f)

engine = create_engine('postgres://', connect_args=config)


