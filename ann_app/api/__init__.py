from flask import Blueprint
import os 
import sys 

api = Blueprint("api", __name__)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from . import (
    billing,
    etl,
    ai
)