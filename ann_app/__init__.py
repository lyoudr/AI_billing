from . import api

from flasgger import Swagger 
from flask_sqlalchemy import SQLAlchemy
from flask import Flask 
from cqlengine import connection
import os 
import sys

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    # Set Flask configuration variables
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://ann:annpasswd@localhost:3306/billing'
    app.config['GCP_PROJECT_ID'] = os.getenv('GCP_PROJECT_ID')
    # Blueprint 
    app.register_blueprint(api.api)
    # DataBase
    db = SQLAlchemy(app)
    # Initialize Swagger
    swagger = Swagger(app)
    return app 

current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.dirname(current_dir)

sys.path.append(current_dir)
sys.path.append(parent_dir)



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)