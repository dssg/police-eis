from flask import Flask
from webapp.flask_util_js import FlaskUtilJs

app = Flask(__name__,instance_relative_config=True)
fujs = FlaskUtilJs(app)

from webapp import controller, query
