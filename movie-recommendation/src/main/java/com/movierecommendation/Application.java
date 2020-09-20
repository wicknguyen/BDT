package com.movierecommendation;

import com.movierecommendation.controllers.HBaseController;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.io.IOException;

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
    RouterFunction<ServerResponse> createTable(HBaseController controller) {
        return RouterFunctions.route(RequestPredicates.GET("/create"),
                request -> {
                    try {
                        return ServerResponse.ok().body(controller.createTable(request), String.class);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return ServerResponse.ok().body(Mono.just("OK"), String.class);
                });
    }

}
