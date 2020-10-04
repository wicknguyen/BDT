package com.movierecommendation.services;

import static com.movierecommendation.constants.Constants.COMMA_SEPARATOR;
import static com.movierecommendation.constants.Constants.GENRES;
import static com.movierecommendation.constants.Constants.HOST_VALUE;
import static com.movierecommendation.constants.Constants.INPUT_MOVIES;
import static com.movierecommendation.constants.Constants.INPUT_RATINGS;
import static com.movierecommendation.constants.Constants.MOVIES_TABLE_NAME;
import static com.movierecommendation.constants.Constants.MOVIE_ID;
import static com.movierecommendation.constants.Constants.PORT_VALUE;
import static com.movierecommendation.constants.Constants.RATING;
import static com.movierecommendation.constants.Constants.RATINGS_TABLE_NAME;
import static com.movierecommendation.constants.Constants.SPARK_HBASE_HOST;
import static com.movierecommendation.constants.Constants.SPARK_HBASE_PORT;
import static com.movierecommendation.constants.Constants.TIMESTAMP;
import static com.movierecommendation.constants.Constants.TITLE;
import static com.movierecommendation.constants.Constants.USER_ID;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import javax.annotation.PostConstruct;

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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.stereotype.Component;

import scala.Tuple2;

@Component
public class HBaseService implements Serializable {

	private static final long serialVersionUID = -9103844571083459164L;
	private static Logger logger = Logger.getLogger(HBaseService.class);

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

        logger.info("Creating HBase tables .... ");
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()) {
            createHBaseTable(movieTable, admin);
            createHBaseTable(ratingTable, admin);
        	loadData(config);
        }

        logger.info("Created HBase tables.");
    }

    private void createHBaseTable(HTableDescriptor tableDescriptor, Admin admin) throws IOException {
        if (admin.tableExists(tableDescriptor.getTableName())) {
        	logger.info(String.format("Deleting existed table [%s]", tableDescriptor.getTableName()));
            admin.disableTable(tableDescriptor.getTableName());
            admin.deleteTable(tableDescriptor.getTableName());
        }
        admin.createTable(tableDescriptor);
        logger.info(String.format("Created table %s", tableDescriptor.getTableName()));
    }

    private void loadData(Configuration conf) {
        JavaSparkContext sparkContext = createSparkContext();

        conf.set(SPARK_HBASE_HOST, HOST_VALUE);
        conf.set(SPARK_HBASE_PORT, PORT_VALUE);
        
        JobConf jobConf = new JobConf(conf, HBaseService.class);
        jobConf.setOutputFormat(TableOutputFormat.class);
        
        loadMoviesDataFromCSVFile(sparkContext, jobConf);
        loadRatingsDataFromCSVFile(sparkContext, jobConf);
    }

    private void loadRatingsDataFromCSVFile(JavaSparkContext sparkContext, JobConf jobConf) {
    	logger.info("Loading Rating data into HBase table");
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, RATINGS_TABLE_NAME);

        JavaPairRDD<ImmutableBytesWritable, Put> ratingData = sparkContext.textFile(INPUT_RATINGS)
                .filter(s -> NumberUtils.isCreatable(s.split(COMMA_SEPARATOR)[0]))
                .mapToPair((PairFunction<String, ImmutableBytesWritable, Put>) s -> {
                    String[] fields = s.split(COMMA_SEPARATOR);
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
    	logger.info("Loading Movie data into HBase table");
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, MOVIES_TABLE_NAME);

        JavaPairRDD<ImmutableBytesWritable, Put> movieData = sparkContext
        		.textFile(INPUT_MOVIES)
                .filter(s -> NumberUtils.isCreatable(s.split(COMMA_SEPARATOR)[0]))
                .mapToPair((PairFunction<String, ImmutableBytesWritable, Put>) s -> {
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
//        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
//            .setAppName("MovieRecommendationHBase")
//            .setMaster("local"));

    	JavaSparkContext sparkContext = new JavaSparkContext(
    			new SparkConf()
    			.setMaster("local[*]")
    			.setAppName("Recommender")
    			.set("spark.executor.memory", "4g")
                .set("spark.network.timeout","1000")
                .set("spark.executor.heartbeatInterval","100"));
        return sparkContext;
    }

}
