from flask import Flask
import json
import os
app = Flask(__name__,instance_relative_config=True)

from webapp import views, query
