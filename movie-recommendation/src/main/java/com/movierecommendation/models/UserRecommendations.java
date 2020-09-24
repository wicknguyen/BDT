package com.movierecommendation.models;

import java.util.Set;

public class UserRecommendations {
    private final int user;
    private final Set<Integer> movieIds;

    public UserRecommendations(int user, Set<Integer> movieIds) {
        this.user = user;
        this.movieIds = movieIds;
    }

    public int getUser() {
        return user;
    }

    public Set<Integer> getMovieIds() {
        return movieIds;
    }
}
