package me.calebe_oliveira.advanced_springbatch.model;

public class AverageScoredTeam {
    private final String name;
    private final double averageScore;

    public AverageScoredTeam(String name, double averageScore) {
        this.name = name;
        this.averageScore = averageScore;
    }

    public String getName() {
        return name;
    }

    public double getAverageScore() {
        return averageScore;
    }
}
