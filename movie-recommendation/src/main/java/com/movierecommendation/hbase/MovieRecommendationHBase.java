package com.movierecommendation.hbase;

import java.io.IOException;
import java.util.Arrays;

import com.movierecommendation.models.Movies;
import com.movierecommendation.spark.SparkConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class MovieRecommendationHBase {

    private static final String MOVIES_TABLE_NAME = "movies";
    private static final String MOVIE_ID = "movieId";
    private static final String TITLE = "title";
    private static final String GENRES = "genres";

    private static final String RATINGS_TABLE_NAME = "ratings";
    private static final String USER_ID = "userId";
    private static final String RATING = "rating";
    private static final String TIMESTAMP = "timestamp";

    private static final String TAGS_TABLE_NAME = "tags";
    private static final String TAG = "tag";

    private static final String LINKS_TABLE_NAME = "links";
    private static final String IMDB_ID = "imdbId";
    private static final String TMDB_ID = "tmdbId";

    @Autowired
    private HBaseClientConfiguration hBaseClientConfiguration;
    @Autowired
    private SparkConfig sparkConfig;

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

        HTableDescriptor tagTable = new HTableDescriptor(TableName.valueOf(TAGS_TABLE_NAME));
        tagTable.addFamily(new HColumnDescriptor(USER_ID).setCompressionType(Algorithm.NONE));
        tagTable.addFamily(new HColumnDescriptor(MOVIE_ID).setCompressionType(Algorithm.NONE));
        tagTable.addFamily(new HColumnDescriptor(TAG).setCompressionType(Algorithm.NONE));
        tagTable.addFamily(new HColumnDescriptor(TIMESTAMP).setCompressionType(Algorithm.NONE));

        HTableDescriptor linkTable = new HTableDescriptor(TableName.valueOf(LINKS_TABLE_NAME));
        linkTable.addFamily(new HColumnDescriptor(MOVIE_ID).setCompressionType(Algorithm.NONE));
        linkTable.addFamily(new HColumnDescriptor(IMDB_ID).setCompressionType(Algorithm.NONE));
        linkTable.addFamily(new HColumnDescriptor(TMDB_ID).setCompressionType(Algorithm.NONE));

        System.out.print("Creating table.... ");

        hBaseClientConfiguration.getAdmin().createTable(movieTable);
        hBaseClientConfiguration.getAdmin().createTable(ratingTable);
        hBaseClientConfiguration.getAdmin().createTable(tagTable);
        hBaseClientConfiguration.getAdmin().createTable(linkTable);

        System.out.println(" Done!");
    }

    private void loadData() {
        JavaRDD<Movies> moviesJavaRDD = sparkConfig.getSparkContext().textFile("input/movies.csv")
                .map((Function<String, Movies>) line -> {
                    String[] fields = line.split(",");
                    return new Movies(Long.parseLong(fields[0]), fields[1], fields[2]);
                });
        JavaPairRDD<ImmutableBytesWritable, Result> javaPairRdd = sparkConfig.getSparkContext()
                .newAPIHadoopRDD(hBaseClientConfiguration.getConnection().getConfiguration(), TableInputFormat.class,ImmutableBytesWritable.class, Result.class);
    }

    public String getValue() throws IOException {
        Configuration config = HBaseConfiguration.create();
        Result row = null;
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            Table tbl = connection.getTable(TableName.valueOf(MOVIES_TABLE_NAME));
            byte[] rowkey = Bytes.toBytes("3"); // select Bob

            row = tbl.get(new Get(rowkey));
        }
        return row.toString();
    }
}
