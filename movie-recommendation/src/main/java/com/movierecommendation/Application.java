package com.movierecommendation;

import com.movierecommendation.constants.Constants;
import com.movierecommendation.controllers.MovieRecommendationController;
import com.movierecommendation.models.Movies;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;

@SpringBootApplication
@EnableWebFlux
public class Application {

    public static void main(String[] args) {
        new SpringApplicationBuilder(Application.class).web(WebApplicationType.REACTIVE).run(args);
    }

    @Bean
    RouterFunction<ServerResponse> systemCheck() {
        return RouterFunctions.route(RequestPredicates.GET("/system/check"),
                request -> ServerResponse.ok().body(Mono.just("OK"), String.class));
    }

    @Bean
    RouterFunction<ServerResponse> createTable(MovieRecommendationController controller) {
        return RouterFunctions.route(RequestPredicates.GET("/user/{" + Constants.USER_ID + "}"),
                request -> {
                    try {
                        return ServerResponse.ok().body(controller.recommendMovies(request), new ParameterizedTypeReference<List<Movies>>() {});
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return ServerResponse.ok().body(Mono.just("OK"), String.class);
                });
    }
    
    @Bean
    RouterFunction<ServerResponse> loadMovie(MovieRecommendationController controller) {
        return RouterFunctions.route(RequestPredicates.GET("/movie/{" + Constants.MOVIE_ID + "}"),
                controller::loadMovie);
    }
    
    @Bean
    RouterFunction<ServerResponse> trainModel(MovieRecommendationController controller) {
        return RouterFunctions.route(RequestPredicates.GET("/train"),
                controller::trainModel);
    }

}
