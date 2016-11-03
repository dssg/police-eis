from flask import Flask

app = Flask(__name__,instance_relative_config=True)

from webapp import controller, query
