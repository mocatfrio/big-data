import os, logging, json
import pandas as pd
import pyspark.sql.functions as func
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecommendationEngine:
  """
  Recommendation engine
  """
  
  def __init__(self, spark_session, dataset_path):
    self.spark_session = spark_session
    logger.info("Starting up the Spark Session: {}".format(self.spark_session))
    
    # Load listening count data
    logger.info("Loading listening count dataset...")
    self.listening_count_df = []
    for i in range(0, 3):
      lc_file_path = os.path.join(dataset_path, 'batch/batch' + str(i) + '.txt')
      new_df = spark_session.read.csv(lc_file_path, header=None, inferSchema=True).na.drop()
      new_df = new_df.selectExpr("_c0 as user_id", "_c1 as artist_id", "_c2 as weight")
      try:
        self.listening_count_df.append(self.listening_count_df[i-1].union(new_df))
      except IndexError:
        self.listening_count_df.append(new_df)
      logger.info("{} loaded".format('batch' + str(i) + '.txt'))  
    logger.info("Loading listening count dataset done!")
    
    # Load artist data
    logger.info("Loading artist dataset...")
    artist_file_path = os.path.join(dataset_path, 'csv/artists.csv')
    self.artist_df = spark_session.read.csv(artist_file_path, header="true", inferSchema="true").na.drop()
    self.artist_df.createOrReplaceTempView("artists")
    self.artist_df_selected = self.spark_session.sql("SELECT `id` as artist_id, `name`, `url` \
                            FROM artists")
    logger.info("Loading artist dataset done...")

    # Train the model
    self.__train_model()

  def __train_model(self):
    # Train the ALS model with the current dataset
    logger.info("Training the ALS model...")
    self.als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="artist_id", ratingCol="weight", coldStartStrategy="drop")
    self.model = []
    for i in range(0, 3):
      self.model.append(self.als.fit(self.listening_count_df[i]))
      logger.info("Model {} done : {}".format(i, self.listening_count_df[i].count()))
    logger.info("ALS model built!")

  def get_top_ratings(self, model_id, user_id, num_of_books):
    # Recommends up to top unrated books to user_id
    user = self.listening_count_df[model_id].select(self.als.getUserCol())
    user = user.filter(user.user_id == user_id)
    userSubsetRecs = self.model[model_id].recommendForUserSubset(user, num_of_books)
    userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
    userSubsetRecs = userSubsetRecs.select(func.col('recommendations')['artist_id'].alias('artist_id'), \
                         func.col('recommendations')['Rating'].alias('Rating')).drop('recommendations')
    userSubsetRecs = userSubsetRecs.drop('Rating')
    userSubsetRecs = userSubsetRecs.join(self.artist_df_selected, ('artist_id'), 'inner')
    df_json = userSubsetRecs.toJSON()
    data = {}
    data['result'] = []
    for row in df_json.collect():
      data['result'].append(json.loads(row))
    return data

  def get_top_music_recommend(self, model_id, artist_id, num_of_users):
    # Recommends up to top unrated books to user_id
    artist = self.listening_count_df[model_id].select(self.als.getItemCol())
    artist = artist.filter(artist.artist_id == artist_id)
    artistSubsetRecs = self.model[model_id].recommendForItemSubset(artist, num_of_users)
    artistSubsetRecs = artistSubsetRecs.withColumn("recommendations", explode("recommendations"))
    artistSubsetRecs = artistSubsetRecs.select(func.col('recommendations')['user_id'].alias('user_id'), \
                        func.col('recommendations')['Rating'].alias('Rating')).drop('recommendations')
    artistSubsetRecs = artistSubsetRecs.drop('Rating')
    df_json = artistSubsetRecs.toJSON()
    data = {}
    data['result'] = []
    for row in df_json.collect():
      data['result'].append(json.loads(row))
    return data

  def get_listening_count_for_artist_ids(self, model_id, user_id, artist_id):
    # Given a user_id and a list of artist_ids, predict listening count for them
    request = self.spark_session.createDataFrame([(user_id, artist_id)], ['user_id', 'artist_id'])
    weight = self.model[model_id].transform(request).collect()
    data = {}
    data['result'] = weight[0][2]
    return data

  def get_listening_count(self, model_id, user_id):
    # Get listening count history for a user
    self.listening_count_df[model_id].createOrReplaceTempView("listeningcount")
    user_history = self.spark_session.sql('SELECT `artist_id`, `weight` from listeningcount \
                        WHERE `user_id` = "%s"' %user_id)
    user_history = user_history.join(self.artist_df_selected, ('artist_id'), 'inner')
    df_json = user_history.toJSON()
    data = {}
    data['result'] = []
    for row in df_json.collect():
      data['result'].append(json.loads(row))
    return data

  
