from flask import Blueprint
main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

# logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    """"
    Untuk menampilkan sejumlah <count> rekomendasi film ke user <user_id>
    """
    logger.debug("User %s TOP ratings requested", user_id)
    top_rated = recommendation_engine.get_top_ratings(user_id, count)
    return json.dumps(top_rated)


@main.route("/books/<int:book_id>/recommend/<int:count>", methods=["GET"])
def movie_recommending(book_id, count):
    """"
    Untuk menampilkan film <book_id> terbaik direkomendasikan ke sejumlah <count> user
    """
    logger.debug("Book ID %s TOP user recommending", book_id)
    top_rated = recommendation_engine.get_top_book_recommend(book_id, count)
    return json.dumps(top_rated)


@main.route("/<int:user_id>/ratings/<int:book_id>", methods=["GET"])
def movie_ratings(user_id, book_id):
    """"
    Untuk melakukan prediksi user <user_id> memberi rating X terhadap film <book_id>
    """
    logger.debug("User %s rating requested for book %s", user_id, book_id)
    ratings = recommendation_engine.get_ratings_for_book_ids(user_id, book_id)
    return json.dumps(ratings)

@main.route("/<int:user_id>/history", methods=["GET"])
def ratings_history(user_id):
    """"
    Untuk melihat riwayat pemberian rating oleh user <user_id>
    """
    logger.debug("History for user %s is requested", user_id)
    user_history = recommendation_engine.get_history(user_id)
    return json.dumps(user_history)

@main.route("/<int:user_id>/giverating", methods=["POST"])
def add_ratings(user_id):
    """"
    Untuk submit <user_id> memberikan rating untuk film X
    """
    # get the ratings from the Flask POST request object
    movie_id_fetched = int(request.form.get('movie_id'))
    ratings_fetched = float(request.form.get('rating_given'))
    # add them to the model using then engine API
    new_rating = recommendation_engine.add_ratings(user_id, movie_id_fetched, ratings_fetched)
    return json.dumps(new_rating)

def create_app(spark_session, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
