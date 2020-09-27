package com.movierecommendation.constants;

import scala.Serializable;

public class Constants implements Serializable {
	
	private static final long serialVersionUID = -8947968614253259046L;
	
	public static final String MOVIES_TABLE_NAME = "movies";
	public static final String MOVIE_ID = "movieId";
	public static final String TITLE = "title";
	public static final String GENRES = "genres";

	public static final String RATINGS_TABLE_NAME = "ratings";
	public static final String USER_ID = "userId";
	public static final String RATING = "rating";
	public static final String TIMESTAMP = "timestamp";

	public static final String TAGS_TABLE_NAME = "tags";
	public static final String TAG = "tag";

	public static final String LINKS_TABLE_NAME = "links";
	public static final String IMDB_ID = "imdbId";
	public static final String TMDB_ID = "tmdbId";
	
    public static final String SPARK_HBASE_HOST = "spark.hbase.host";
    public static final String HOST_VALUE = "localhost";
    public static final String SPARK_HBASE_PORT = "spark.hbase.port";
    public static final String PORT_VALUE = "2181";
    public static final String INPUT_MOVIES = "src/main/resources/input/movies.csv";
    public static final String COMMA_SEPARATOR = ",";
    public static final String INPUT_RATINGS = "src/main/resources/input/ratings.csv";

}
