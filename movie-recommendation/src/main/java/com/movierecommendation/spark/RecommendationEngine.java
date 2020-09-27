package com.movierecommendation.spark;

import com.movierecommendation.models.UserRecommendations;
import com.movierecommendation.spark.als.ModelFactory;
import com.movierecommendation.spark.als.TrainConfig;
import com.movierecommendation.spark.als.TrainedModel;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

import java.util.HashSet;
import java.util.Set;

public class RecommendationEngine {
	private static Logger logger = Logger.getLogger(RecommendationEngine.class);

    public TrainedModel train(TrainConfig trainConfig, JavaRDD<Rating> ratings) {
        logger.info("Start training model");
        return createAlsModel(ratings, trainConfig);
    }

    private TrainedModel createAlsModel(JavaRDD<Rating> ratings, TrainConfig trainConfig) {
        double[] weights = {8, 2};
        JavaRDD<Rating>[] randomRatings = ratings.randomSplit(weights, 0L);

        return ModelFactory.create(randomRatings[0],
                randomRatings[1],
                trainConfig.getRankNr(),
                trainConfig.getIterationsNr()
        );
    }

    public JavaRDD<UserRecommendations> recommendMoviesForUser(TrainedModel model, int userId) {
        logger.info("start user recommendations");
        JavaRDD<Tuple2<Object, Rating[]>> recommendations = model.getMatrixModel()
                .recommendProductsForUsers(userId)
                .toJavaRDD();

        logger.info("recommendations count " + recommendations.count());

        JavaRDD<UserRecommendations> userRecommendationsRDD = recommendations.map(tuple -> {
            Set<Integer> products = new HashSet<>();
            for (Rating rating : tuple._2) {
                products.add(rating.product());
            }

            return new UserRecommendations((int) tuple._1(), products);
        });
        return userRecommendationsRDD;
    }
}
