package com.movierecommendation.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import scala.Serializable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

//@Configuration
public class SparkConfig {

    private JavaSparkContext sparkContext;

//    @PostConstruct
    public void init() {
        this.sparkContext = new JavaSparkContext(new SparkConf()
                .setAppName("MovieRecommendation")
                .setMaster("local"));
    }

//    @PreDestroy
    public void onDestroy() {
        this.sparkContext.close();
    }
}
