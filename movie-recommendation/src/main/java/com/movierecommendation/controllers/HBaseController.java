package com.movierecommendation.controllers;

import com.movierecommendation.hbase.MovieRecommendationHBase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.server.ServerRequest;
import reactor.core.publisher.Mono;

import java.io.IOException;

@RestController
public class HBaseController {

    @Autowired
    private MovieRecommendationHBase movieRecommendationHBase;

    public Mono<String> createTable(ServerRequest request) throws IOException {
        movieRecommendationHBase.initTables();
        return Mono.just(movieRecommendationHBase.getValue());
    }
}
