package com.movierecommendation.hbase;

import static com.movierecommendation.constants.Constants.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import com.movierecommendation.models.Movies;
import com.movierecommendation.models.Ratings;
import com.movierecommendation.models.UserRecommendations;
import com.movierecommendation.spark.RecommendationEngine;
import com.movierecommendation.spark.als.ModelFinder;
import com.movierecommendation.spark.als.TrainConfig;
import com.movierecommendation.spark.als.TrainedModel;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.Rating;
import org.springframework.stereotype.Component;

import scala.Tuple2;

import javax.annotation.PostConstruct;

@Component
public class MovieRecommendationHBase implements Serializable {
//    @Autowired
//    private HBaseClientConfiguration hBaseClientConfiguration;
//    @Autowired
//    private SparkConfig sparkConfig;
    
//    private JavaSparkContext sparkContext;

    @PostConstruct
    public void initTables() throws IOException {

        HTableDescriptor movieTable = new HTableDescriptor(TableName.valueOf(MOVIES_TABLE_NAME));
        movieTable.addFamily(new HColumnDescriptor(MOVIE_ID).setCompressionType(Algorithm.NONE));
        movieTable.addFamily(new HColumnDescriptor(TITLE).setCompressionType(Algorithm.NONE));
        movieTable.addFamily(new HColumnDescriptor(GENRES).setCompressionType(Algorithm.NONE));

        HTableDescriptor ratingTable = new HTableDescriptor(TableName.valueOf(RATINGS_TABLE_NAME));
        ratingTable.addFamily(new HColumnDescriptor(USER_ID).setCompressionType(Algorithm.NONE));
        ratingTable.addFamily(new HColumnDescriptor(MOVIE_ID).setCompressionType(Algorithm.NONE));
        ratingTable.addFamily(new HColumnDescriptor(RATING).setCompressionType(Algorithm.NONE));
        ratingTable.addFamily(new HColumnDescriptor(TIMESTAMP).setCompressionType(Algorithm.NONE));

//        HTableDescriptor tagTable = new HTableDescriptor(TableName.valueOf(TAGS_TABLE_NAME));
//        tagTable.addFamily(new HColumnDescriptor(USER_ID).setCompressionType(Algorithm.NONE));
//        tagTable.addFamily(new HColumnDescriptor(MOVIE_ID).setCompressionType(Algorithm.NONE));
//        tagTable.addFamily(new HColumnDescriptor(TAG).setCompressionType(Algorithm.NONE));
//        tagTable.addFamily(new HColumnDescriptor(TIMESTAMP).setCompressionType(Algorithm.NONE));
//
//        HTableDescriptor linkTable = new HTableDescriptor(TableName.valueOf(LINKS_TABLE_NAME));
//        linkTable.addFamily(new HColumnDescriptor(MOVIE_ID).setCompressionType(Algorithm.NONE));
//        linkTable.addFamily(new HColumnDescriptor(IMDB_ID).setCompressionType(Algorithm.NONE));
//        linkTable.addFamily(new HColumnDescriptor(TMDB_ID).setCompressionType(Algorithm.NONE));

        System.out.print("Creating table.... ");
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()) {
            createHBaseTable(movieTable, admin);
            createHBaseTable(ratingTable, admin);
        	loadData(config);
        }

        System.out.println(" Done!");
    }

    private void createHBaseTable(HTableDescriptor tableDescriptor, Admin admin) throws IOException {
        if (admin.tableExists(tableDescriptor.getTableName())) {
            admin.disableTable(tableDescriptor.getTableName());
            admin.deleteTable(tableDescriptor.getTableName());
        }
        admin.createTable(tableDescriptor);
    }

    private void loadData(Configuration conf) {
        JavaSparkContext sparkContext = createSparkContext();

        conf.set(SPARK_HBASE_HOST, HOST_VALUE);
        conf.set(SPARK_HBASE_PORT, PORT_VALUE);
        // exampleClass - a class whose containing jar is used as the job's jar.
        JobConf jobConf = new JobConf(conf, MovieRecommendationHBase.class);
        jobConf.setOutputFormat(TableOutputFormat.class);
        
        // load movies data to hbase
        loadMoviesDataFromCSVFile(sparkContext, jobConf);

        // load rating data to hbase
        loadRatingsDataFromCSVFile(sparkContext, jobConf);
    }

    private void loadRatingsDataFromCSVFile(JavaSparkContext sparkContext, JobConf jobConf) {
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, RATINGS_TABLE_NAME);

        List<String> rawRatingData = sparkContext.textFile(INPUT_RATINGS)
                .filter(s -> NumberUtils.isCreatable(s.split(COMMA_SEPARATOR)[0]))
                .collect();
        JavaPairRDD<ImmutableBytesWritable, Put> ratingData = sparkContext
        		.parallelize(rawRatingData).mapToPair((PairFunction<String, ImmutableBytesWritable, Put>) s -> {
                    String[] fields = s.split(",");
                    if (fields.length != 4) {
                        System.out.println(s);
                    }
                    Put put = new Put(Bytes.toBytes(fields[0] + fields[1]));
                    addColumnData(put, USER_ID, fields[0]);
                    addColumnData(put, MOVIE_ID, fields[1]);
                    addColumnData(put, RATING, fields[2]);
                    addColumnData(put, TIMESTAMP, fields[3]);
                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });
        ratingData.saveAsHadoopDataset(jobConf);
    }

    private void loadMoviesDataFromCSVFile(JavaSparkContext sparkContext, JobConf jobConf) {
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, MOVIES_TABLE_NAME);

        List<String> rawMovieData = sparkContext.textFile(INPUT_MOVIES)
                .filter(s -> NumberUtils.isCreatable(s.split(COMMA_SEPARATOR)[0]))
                .collect();
        JavaPairRDD<ImmutableBytesWritable, Put> movieData = sparkContext
        		.parallelize(rawMovieData).mapToPair((PairFunction<String, ImmutableBytesWritable, Put>) s -> {
                    String[] fields = s.split(COMMA_SEPARATOR);
                    Put put = new Put(Bytes.toBytes(fields[0]));
                    addColumnData(put, MOVIE_ID, fields[0]);
                    addColumnData(put, TITLE, fields[1]);
                    addColumnData(put, GENRES, fields[2]);
                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });
        movieData.saveAsHadoopDataset(jobConf);
    }

    private void addColumnData(Put put, String column, String value) {
        put.addColumn(Bytes.toBytes(column), Bytes.toBytes(column), Bytes.toBytes(value));
    }

    private JavaSparkContext createSparkContext() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
            .setAppName("MovieRecommendationHBase")
            .setMaster("local"));

//    	JavaSparkContext sparkContext = new JavaSparkContext(
//    			new SparkConf()
//    			.setMaster("local[*]")
//    			.setAppName("Recommender")
//    			.set("spark.executor.memory", "4g")
//                .set("spark.network.timeout","1000")
//                .set("spark.executor.heartbeatInterval","100"));
        return sparkContext;
    }

    /*public String getValue(String userId) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, RATINGS_TABLE_NAME);
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
        JavaPairRDD<ImmutableBytesWritable, Result> ratingsRDD = sparkContext
                .newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        JavaRDD<Ratings> rawRatings = ratingsRDD.map(t -> Ratings.toRatings(t._2()));
        JavaRDD<Rating> ratings = rawRatings.map(Ratings::toSparkRating);

        TrainConfig trainConfig = new TrainConfig(10, 4);
        RecommendationEngine recommendationEngine = new RecommendationEngine(sparkContext);
        TrainedModel trainedModel = recommendationEngine.train(trainConfig, ratings);
        new ModelFinder().findBestModel(ratings);
        JavaRDD<UserRecommendations> userRecommendations = recommendationEngine.recommendMoviesForUser(trainedModel, Integer.parseInt(userId));
        List<Integer> moviesIds = userRecommendations
                .map(r -> r.getMovieIds())
                .take(20)
                .stream()
                .flatMap(s -> s.stream())
                .collect(Collectors.toList());

        // query movie
        config.set(TableInputFormat.INPUT_TABLE, MOVIES_TABLE_NAME);
        JavaPairRDD<ImmutableBytesWritable, Result> moviesRDD = sparkContext
                .newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        return moviesRDD.map(t -> Movies.toMovies(t._2()))
                .filter(m -> moviesIds.contains(m.getMovieId()))
                .map(m -> m.getTitle())
                .collect()
                .stream()
                .reduce("Movies size = " + moviesIds.size(), (a, b) -> a + "</br>" + b);
    }*/
}
