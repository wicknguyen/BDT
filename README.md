# Big Data Technology
* BDT final project - Movie Recommendation: 
 Build movie recommendation system to recommend movies for user based on the rating of each movie. This system using movielens data set to train model.
* Collaborative Filtering approaches building a model from a user’s past behavior (items previously purchased or selected and/or numerical ratings given to those items) as well as similar decisions made by other users. This model is then used to predict items (or ratings for items) that the user may have an interest in.

## Technologies
* Java Spark, Spark MLlib for recommendation.
* HBase
* Spring Boot Reactive web

## Data sets
* Movielens data set

# Project Parts Details
## Part 1: Spark Streaming
* Spark streaming to load data from csv data sets
* Spark MLlib to train recommend model, predict as well as recommend movies for user
## Part 2: Integrate with HBase
* Spark streaming read data from CSV files and save to HBase
* Spark streaming read data from HBase to return result and give data for recommendation model
## Part 3: Research new tools: tried to integrate with Kafka but I faced some errors when setting it in Cloudera VMware
 * This is an improvement if have time. Which is integrate with Kafka to get realtime movies data and get the update from user when they are using Movie Recommendation website. 

# How to run project
* This project will be ran in Cloudera VMware, make sure HBase services are started.
* Run file Application.java, there are 3 APIs exposed:
* http://localhost:8080/train - trigger trainning model
* http://localhost:8080/movie/{movieId} - load movie details
* http://localhost:8080/user/{userId} -  get list recommended movie for user, top 10 movies for user based on ratings.
* Input file at "movie-recommendation/src/main/resources/input/"
## References
* https://dzone.com/articles/recommendation-system-using-spark-ml-akka-and-cass#:~:text=Apache%20Spark%20ML%20implements%20alternating,Regularization%20(ALS%2DWR).
* https://medium.com/edureka/spark-mllib-e87546ac268
* https://spark.apache.org/docs/2.2.0/ml-collaborative-filtering.html
* https://github.com/rashnil-git/Recommender-System.git
* https://github.com/cosminseceleanu/movie-recommender.git
* Slides from BDT course.
* Data set: https://www.kaggle.com/shubhammehta21/movie-lens-small-latest-dataset


