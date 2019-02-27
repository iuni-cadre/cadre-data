from flask import Blueprint
import logging.config

blueprint = Blueprint('mag', __name__)
logger = logging.getLogger('mag')