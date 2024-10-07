package me.calebe_oliveira.advanced_springbatch.model;

import java.util.LinkedList;
import java.util.List;

public class ScoredPlayer {
    private final String name;
    private final List<Double> scores = new LinkedList<>();

    public ScoredPlayer(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public List<Double> getScores() {
        return scores;
    }
}
