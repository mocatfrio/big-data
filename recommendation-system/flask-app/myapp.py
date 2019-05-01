import os, math, csv
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

# Create Spark Session
app_name = "Recommendation System"
spark = SparkSession.builder.appName(app_name).getOrCreate()

### Load datasets
# Load the ratings dataset
ratings_file = os.path.join(os.getcwd(), 'csv', 'ratings.csv')
ratings_df = spark.read.load(ratings_file, format="csv", sep=",", inferSchema="true", header="true").na.drop()

# For a reason, I wan't to change the csv separator of books dataset from comma to semicolon 
books_file = os.path.join(os.getcwd(), 'csv', 'books.csv')
books_file_converted = os.path.join(os.getcwd(), 'csv', 'books_converted.csv')
reader = list(csv.reader(open(books_file, "r"), delimiter=','))
writer = csv.writer(open(books_file_converted, 'w'), delimiter=';')
writer.writerows(row for row in reader)

# Load the books dataset
books_df = spark.read.load(books_file_converted, format="csv", sep=";", inferSchema="true", header="true").na.drop()
# Just take book_id and title columns
books_df.createOrReplaceTempView("books")
books_df_selected = spark.sql("SELECT `book_id`, `title` \
                                FROM books")

# Check number of data
print ("There are " + str(ratings_df.count()) + " ratings in this dataset")
print ("There are " + str(books_df_selected.count()) + " books in this dataset")

### ALS Algorithm
# Split ratings data become training set and test set
(training, test) = ratings_df.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="book_id", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))