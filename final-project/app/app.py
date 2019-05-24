from flask import Blueprint
main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

# logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

"""
Menampilkan sejumlah <count> rekomendasi film ke user <user_id> pada model <model>
"""
@main.route("/<int:model_id>/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(model_id, user_id, count):
  logger.debug("User %s TOP ratings requested", user_id)
  top_rated = recommendation_engine.get_top_ratings(model_id, user_id, count)
  return json.dumps(top_rated)

"""
Menampilkan music dengan <artist_id> terbaik direkomendasikan ke sejumlah <count> user
"""
@main.route("/<int:model_id>/artists/<int:artist_id>/recommend/<int:count>", methods=["GET"])
def artist_recommending(model_id, artist_id, count):
  logger.debug("Book ID %s TOP user recommending", artist_id)
  top_rated = recommendation_engine.get_top_music_recommend(model_id, artist_id, count)
  return json.dumps(top_rated)

"""
Melakukan prediksi user <user_id> mendengarkan sejumlah X terhadap artist <artist_id>
"""
@main.route("/<int:model_id>/<int:user_id>/listen/<int:artist_id>", methods=["GET"])
def listening_count(model_id, user_id, artist_id):
  logger.debug("User %s rating requested for book %s", user_id, artist_id)
  weight = recommendation_engine.get_listening_count_for_artist_ids(model_id, user_id, artist_id)
  return json.dumps(weight)

"""
Melihat riwayat listening oleh user <user_id>
"""
@main.route("/<int:model_id>/<int:user_id>/history", methods=["GET"])
def listening_history(model_id, user_id):
  logger.debug("History for user %s is requested", user_id)
  user_history = recommendation_engine.get_listening_count(model_id, user_id)
  return json.dumps(user_history)

def create_app(spark_session, dataset_path):
  global recommendation_engine

  recommendation_engine = RecommendationEngine(spark_session, dataset_path)

  app = Flask(__name__)
  app.register_blueprint(main)
  return app
