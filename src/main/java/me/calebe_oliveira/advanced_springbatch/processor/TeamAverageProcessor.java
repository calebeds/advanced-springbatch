package me.calebe_oliveira.advanced_springbatch.processor;

import me.calebe_oliveira.advanced_springbatch.model.AverageScoredTeam;
import me.calebe_oliveira.advanced_springbatch.model.ScoredPlayer;
import me.calebe_oliveira.advanced_springbatch.model.Team;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;

public class TeamAverageProcessor implements ItemProcessor<Team, AverageScoredTeam> {
    public static final String MAX_SCORE = "max.score";
    public static final String MAX_PLAYER = "max.player";
    public static final String MIN_SCORE = "min.score";
    public static final String MIN_PLAYER = "min.player";

    private final int scoreRank;
    private StepExecution stepExecution;

    public TeamAverageProcessor(int scoreRank) {
        this.scoreRank = scoreRank;
    }

    public void setStepExecution(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public AverageScoredTeam process(Team team) throws Exception {
        if(stepExecution == null) {
            throw new RuntimeException("Team average processor can not execute without step execution set");
        }

        ExecutionContext stepContext = stepExecution.getExecutionContext();

        Double maxScore = stepContext.containsKey(MAX_SCORE) ? stepContext.getDouble(MAX_SCORE) : null;
        Double minScore = stepContext.containsKey(MIN_SCORE) ? stepContext.getDouble(MIN_SCORE) : null;

        double sum = 0;
        double count = 0;
        for(ScoredPlayer scoredPlayer: team.getScoredPlayers()) {
            double score = scoredPlayer.getScores().get(scoreRank);

            if(maxScore == null || score > maxScore) {
                stepContext.putDouble(MAX_SCORE, score);
                stepContext.putString(MAX_PLAYER, scoredPlayer.getName());
                maxScore = score;
            }

            if(minScore == null || score < minScore) {
                stepContext.putDouble(MIN_SCORE, score);
                stepContext.putString(MIN_PLAYER, scoredPlayer.getName());
                minScore = score;
            }

            sum += score;
            count++;
        }

        return new AverageScoredTeam(team.getName(), sum/count);
    }
}
