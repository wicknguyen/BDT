package com.movierecommendation.services;

import static com.movierecommendation.constants.Constants.MOVIES_TABLE_NAME;
import static com.movierecommendation.constants.Constants.RATINGS_TABLE_NAME;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.Rating;
import org.springframework.stereotype.Service;

import com.movierecommendation.models.Movies;
import com.movierecommendation.models.Ratings;
import com.movierecommendation.models.UserRecommendations;
import com.movierecommendation.spark.RecommendationEngine;
import com.movierecommendation.spark.als.TrainConfig;
import com.movierecommendation.spark.als.TrainedModel;

@Service
public class RecommendationEngineService {
	
	private TrainedModel trainedModel;
	
	public void trainModel() {
		Configuration config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, RATINGS_TABLE_NAME);
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
        JavaPairRDD<ImmutableBytesWritable, Result> ratingsRDD = sparkContext
                .newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        JavaRDD<Ratings> rawRatings = ratingsRDD.map(t -> Ratings.toRatings(t._2()));
        JavaRDD<Rating> ratings = rawRatings.map(Ratings::toSparkRating);

        TrainConfig trainConfig = new TrainConfig(10, 4);
        this.trainedModel = new RecommendationEngine().train(trainConfig, ratings);
	}
	
	public List<Movies> recommendMovies(String userId) throws IOException {
        if (this.trainedModel == null) {
        	trainModel();
        }
        
        JavaRDD<UserRecommendations> userRecommendations = new RecommendationEngine().recommendMoviesForUser(trainedModel, Integer.parseInt(userId));
        List<Integer> moviesIds = userRecommendations
                .flatMap(r -> r.getMovieIds().iterator())
                .take(10);

        // query movies
        Configuration config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, MOVIES_TABLE_NAME);
        
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
        JavaPairRDD<ImmutableBytesWritable, Result> moviesRDD = sparkContext
                .newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        return moviesRDD.map(t -> Movies.toMovies(t._2()))
                .filter(m -> moviesIds.contains(m.getMovieId()))
                .collect();
    }

}
