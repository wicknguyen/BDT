package com.movierecommendation.models;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.mllib.recommendation.Rating;

import com.movierecommendation.constants.Constants;

public class Ratings implements Serializable {

	private static final long serialVersionUID = 8891605555616953081L;
	
	private int userId;
    private int movieId;
    private double rating;
    private long timestamp;

    
    public Ratings() {
		super();
	}

	public Ratings(int userId, int movieId, double rating, long timestamp) {
		super();
		this.userId = userId;
		this.movieId = movieId;
		this.rating = rating;
		this.timestamp = timestamp;
	}

	public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    public static Ratings toRatings(Result result) {
    	Ratings ratings = new Ratings();
    	ratings.movieId = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes(Constants.MOVIE_ID), Bytes.toBytes(Constants.MOVIE_ID))));
    	ratings.userId = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes(Constants.USER_ID), Bytes.toBytes(Constants.USER_ID))));
    	ratings.rating = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes(Constants.RATING), Bytes.toBytes(Constants.RATING))));
    	ratings.timestamp = Long.parseLong(Bytes.toString(result.getValue(Bytes.toBytes(Constants.TIMESTAMP), Bytes.toBytes(Constants.TIMESTAMP))));
    	return ratings;
    }

	@Override
	public String toString() {
		return "Ratings [userId=" + userId + ", movieId=" + movieId
				+ ", rating=" + rating + ", timestamp=" + timestamp + "]";
	}
	
	public Rating toSparkRating() {
        return new Rating(userId, movieId, rating);
    }

    public static Ratings fromSparkRating(Rating rating) {
        return new Ratings(rating.user(), rating.product(), rating.rating(), 1L);
    }
    
    
    
}
