### Assignment 04
# Book Recommendation System using Apache Spark and Flask

## 1. Preparation
### 1.1 Requirements
1. Apache Spark 2.4.0 Binary
2. PySpark 2.4.2 (Apache Spark Python API)
3. Jupyter Notebook 
4. Numpy 1.16.3
5. Flask
6. Postman

### 1.2 Dataset
* Dataset's name: [Goodbooks-10k Datasets](https://github.com/zygmuntz/goodbooks-10k)
* Description : This dataset contains six million ratings for ten thousand most popular books (with most ratings). There are a few types of data here: 
    * Explicit ratings 
    * Implicit feedback indicators (books marked to read)
    * Tabular data or metadata (book info)
    * Tags
* Since we only need explicit ratings and book's metadata, so, we'll only use two kinds of data i.e. **ratings.csv** and **books.csv**

## 2. Description

Book recommendation system is implemented using collaborative filtering with Spark's Alternating Least Squares (ALS) algorithm and Flask Python micro-framework. It's divided into two parts, that is:

### Part 1 : [Build a recommender engine using ALS Algorithm](notebook/04%20-%20Recommendation%20System.ipynb) 
### Part 2 : [Build a web-service on top of Spark models using Flask](recommendation-system.md)

## References
* https://spark.apache.org/docs/latest/ml-collaborative-filtering.html
* https://github.com/jadianes/spark-movie-lens/blob/master/notebooks/online-recommendations.ipynb
* https://www.codementor.io/jadianes/building-a-recommender-with-apache-spark-python-example-app-part1-du1083qbw
