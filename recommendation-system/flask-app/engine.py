import os, logging
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
    A book recommendation engine
    """
    
    def __init__(self, spark_session, dataset_path):
        """
        Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Recommendation Engine: ")
        self.spark_session = spark_session
        # Load ratings data for later use
        logger.info("Loading Ratings dataset...")
        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        self.ratings_df = spark_session.read.csv(ratings_file_path, header="true", inferSchema="true").na.drop()
        # Load movies data for later use
        logger.info("Loading Books dataset...")
        books_file_path = os.path.join(dataset_path, 'books.csv')
        self.books_df = spark_session.read.csv(books_file_path, header="true", inferSchema="true").na.drop()
        self.books_df.createOrReplaceTempView("books")
        self.books_df_selected = self.spark_session.sql("SELECT `book_id`, `title` \
                                                        FROM books")
        # Train the model
        self.__train_model()

    def __train_model(self):
        """
        Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="book_id", ratingCol="rating", coldStartStrategy="drop")
        self.model = self.als.fit(self.ratings_df)
        logger.info("ALS model built!")

    def get_top_ratings(self, user_id, book_count):
        """
        Recommends up to book_count top unrated books to user_id
        """
        users = self.ratings_df.select(self.als.getUserCol())
        users = users.filter(users.user_id == user_id)
        userSubsetRecs = self.model.recommendForUserSubset(users, book_count)
        userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
        userSubsetRecs = userSubsetRecs.select(func.col('user_id'), \
                                               func.col('recommendations')['book_id'].alias('book_id'), \
                                               func.col('recommendations')['Rating'].alias('Rating')).drop('recommendations')
        userSubsetRecs = userSubsetRecs.drop('Rating')
        userSubsetRecs = userSubsetRecs.join(self.books_df_selected, ("book_id"), 'inner')
        userSubsetRecs = userSubsetRecs.toPandas()
        userSubsetRecs = userSubsetRecs.to_json()
        return userSubsetRecs

    def get_top_book_recommend(self, book_id, user_count):
        """
        Recommends up to book_count top unrated books to user_id
        """
        books = self.ratings_df.select(self.als.getItemCol())
        books = books.filter(books.book_id == book_id)
        bookSubsetRecs = self.model.recommendForItemSubset(books, user_count)
        bookSubsetRecs = bookSubsetRecs.withColumn("recommendations", explode("recommendations"))
        bookSubsetRecs = bookSubsetRecs.select(func.col('book_id'), \
                                                func.col('recommendations')['user_id'].alias('user_id'), \
                                                func.col('recommendations')['Rating'].alias('Rating')).drop('recommendations')
        bookSubsetRecs = bookSubsetRecs.drop('Rating')
        bookSubsetRecs = bookSubsetRecs.join(self.books_df_selected, ("book_id"), 'inner')
        bookSubsetRecs = bookSubsetRecs.toPandas()
        bookSubsetRecs = bookSubsetRecs.to_json()
        return bookSubsetRecs

    def get_ratings_for_book_ids(self, user_id, book_id):
        """
        Given a user_id and a list of book_ids, predict ratings for them
        """
        request = self.spark_session.createDataFrame([(user_id, book_id)], ["user_id", "book_id"])
        ratings = self.model.transform(request).collect()
        return ratings

    def add_ratings(self, user_id, book_id, ratings_given):
        """
        Add additional movie ratings in the format (user_id, movie_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings = self.spark_session.createDataFrame([(user_id, book_id, ratings_given)],
                                                         ["user_id", "book_id", "rating"])
        # Add new ratings to the existing ones
        self.ratings_df = self.ratings_df.union(new_ratings)
        # Re-train the ALS model with the new ratings
        self.__train_model()
        new_ratings = new_ratings.toPandas()
        new_ratings = new_ratings.to_json()
        return new_ratings

    def get_history(self, user_id):
        """
        Get rating history for a user
        """
        self.ratings_df.createOrReplaceTempView("ratingsdata")
        user_history = self.spark_session.sql('SELECT `user_id`, `book_id`, `rating` from ratingsdata \
                                                WHERE `user_id` = "%s"' %user_id)
        user_history = user_history.join(self.books_df_selected, ("book_id"), 'inner')
        user_history = user_history.toPandas()
        user_history = user_history.to_json()
        return user_history

    
