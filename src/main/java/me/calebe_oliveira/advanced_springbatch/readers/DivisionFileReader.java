package me.calebe_oliveira.advanced_springbatch.readers;

import me.calebe_oliveira.advanced_springbatch.model.ScoredPlayer;
import me.calebe_oliveira.advanced_springbatch.model.Team;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;

import java.util.Optional;

public class DivisionFileReader implements ResourceAwareItemReaderItemStream<Team> {

    private final FlatFileItemReader<String> delegateReader;

    public DivisionFileReader(FlatFileItemReader<String> delegateReader) {
        this.delegateReader = delegateReader;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.delegateReader.open(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        this.delegateReader.close();
    }

    @Override
    public void setResource(Resource resource) {
        delegateReader.setResource(resource);
    }

    @Override
    public Team read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        Optional<Team> maybeTeam = Optional.empty();
        String line;

        while ((line = delegateReader.read()) != null) {
            line = line.trim();
            if(line.isEmpty()) {
                return maybeTeam.orElse(null);
            } else if(!line.contains(":")) {
                maybeTeam = Optional.of(new Team(line));
            } else {
                final String[] nameAndScores = line.split(":");
                maybeTeam.ifPresent(team -> team.getScoredPlayers().add(parseScoredPlayer(nameAndScores)));
            }
        }

        return maybeTeam.orElse(null);
    }

    private ScoredPlayer parseScoredPlayer(String[] nameAndScores) {
        String name = nameAndScores[0];
        String[] scores = nameAndScores[1].split(",");
        ScoredPlayer scoredPlayer = new ScoredPlayer(name);

        for (String score: scores) {
            scoredPlayer.getScores().add(Double.parseDouble(score));
        }

        return scoredPlayer;
    }
}
