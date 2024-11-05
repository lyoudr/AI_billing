from . import api

from flasgger import Swagger 
from flask_sqlalchemy import SQLAlchemy
from flask import Flask
import os 

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    # Set Flask configuration variables
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQLALCHEMY_DATABASE_URI')
    app.config['GCP_PROJECT_ID'] = os.getenv('GCP_PROJECT_ID')
    # Blueprint 
    app.register_blueprint(api.api)
    # DataBase
    SQLAlchemy(app)
    # Initialize Swagger
    Swagger(app)
    return app 