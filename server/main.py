import os

from flask import Flask, jsonify
from flask_pymongo import PyMongo

application = Flask(__name__)

# TODO: Setup the factory method
application.config["MONGO_URI"] = (
    "mongodb://"
    + os.environ["DB_USER"]
    + ":"
    + os.environ["DB_PASS"]
    + "@"
    + os.environ["DB_HOST"]
    + ":27017/"
    + os.environ["DB_NAME"]
)

mongo = PyMongo(application)
db = mongo.db


@application.route("/")
def index():
    return jsonify(
        status=True, message=f"Backend server listening....DB_NAME: {db.name}"
    )


if __name__ == "__main__":
    ENVIRONMENT_DEBUG = os.environ.get("APP_DEBUG", True)
    ENVIRONMENT_PORT = os.environ.get("APP_PORT", 5000)
    application.run(host="0.0.0.0", port=ENVIRONMENT_PORT, debug=ENVIRONMENT_DEBUG)
