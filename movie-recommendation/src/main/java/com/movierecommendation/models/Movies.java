package com.movierecommendation.models;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.movierecommendation.constants.Constants;


public class Movies implements Serializable {
    private int movieId;
    private String title;
    private String genres;

    public Movies() {}

    public Movies(int movieId, String title, String genres) {
        this.movieId = movieId;
        this.title = title;
        this.genres = genres;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }
    
    public static Movies toMovies(Result result) {
    	Movies movies = new Movies();
    	movies.movieId = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes(Constants.MOVIE_ID), Bytes.toBytes(Constants.MOVIE_ID))));
    	movies.title = Bytes.toString(result.getValue(Bytes.toBytes(Constants.TITLE), Bytes.toBytes(Constants.TITLE)));
    	movies.genres = Bytes.toString(result.getValue(Bytes.toBytes(Constants.GENRES), Bytes.toBytes(Constants.GENRES)));
    	return movies;
    }

	@Override
	public String toString() {
		return "Movies [movieId=" + movieId + ", title=" + title + ", genres="
				+ genres + "]";
	}
    
    
}
