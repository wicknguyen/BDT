package com.movierecommendation.services;

import static com.movierecommendation.constants.Constants.MOVIES_TABLE_NAME;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import com.movierecommendation.models.Movies;

@Service
public class MovieService {
	
	public Movies loadMovie(String movieId) {
		JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
		Configuration config = HBaseConfiguration.create();
		config.set(TableInputFormat.INPUT_TABLE, MOVIES_TABLE_NAME);
		JavaPairRDD<ImmutableBytesWritable, Result> moviesRDD = sparkContext
		        .newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		return moviesRDD.map(t -> Movies.toMovies(t._2()))
                .filter(m -> movieId.equals(String.valueOf(m.getMovieId())))
                .first();
	}

}
