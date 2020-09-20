package com.movierecommendation.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
public class SparkConfig {

    private JavaSparkContext sparkContext;

    @PostConstruct
    public void init() {
        this.sparkContext = new JavaSparkContext(new SparkConf()
                .setAppName("MovieRecommendation")
                .setMaster("local"));
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }

    @PreDestroy
    public void onDestroy() {
        this.sparkContext.close();
    }
}
