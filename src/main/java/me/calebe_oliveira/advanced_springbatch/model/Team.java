package me.calebe_oliveira.advanced_springbatch.model;

import java.util.LinkedList;
import java.util.List;

public class Team {
    private final String name;
    private final List<ScoredPlayer> scoredPlayers = new LinkedList<>();

    public Team(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public List<ScoredPlayer> getScoredPlayers() {
        return scoredPlayers;
    }
}
