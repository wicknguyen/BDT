package com.movierecommendation.controllers;

import com.movierecommendation.constants.Constants;
import com.movierecommendation.models.Movies;
import com.movierecommendation.services.HBaseService;
import com.movierecommendation.services.MovieService;
import com.movierecommendation.services.RecommendationEngineService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

@RestController
public class MovieRecommendationController implements Serializable {

	private static final long serialVersionUID = -6274103634653122061L;
	
	@Autowired
    private HBaseService movieRecommendationHBase;
    @Autowired
    private MovieService movieRecommendationService;
    @Autowired
    private RecommendationEngineService recommendationEngineService;

    public Mono<List<Movies>> recommendMovies(ServerRequest request) throws IOException {
        String userId = request.pathVariable(Constants.USER_ID);
        return Mono.just(recommendationEngineService.recommendMovies(userId));
    }
    
    public Mono<ServerResponse> trainModel(ServerRequest request) {
    	recommendationEngineService.trainModel();
    	return ServerResponse.ok().body(Mono.just("Trainning model..."), String.class);
    }
    
    public Mono<ServerResponse> loadMovie(ServerRequest request) {
    	return ServerResponse.ok().body(Mono.just(movieRecommendationService.loadMovie(request.pathVariable(Constants.MOVIE_ID))), Movies.class);
    }
    
}
