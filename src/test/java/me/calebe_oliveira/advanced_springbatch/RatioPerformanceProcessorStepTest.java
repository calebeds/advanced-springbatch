package me.calebe_oliveira.advanced_springbatch;

import me.calebe_oliveira.advanced_springbatch.config.TeamPerformanceJobConfiguration;
import me.calebe_oliveira.advanced_springbatch.model.AverageScoredTeam;
import me.calebe_oliveira.advanced_springbatch.model.TeamPerformance;
import me.calebe_oliveira.advanced_springbatch.processor.TeamAverageProcessor;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.test.StepScopeTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBatchTest
@ContextConfiguration(classes = {TeamPerformanceJobConfiguration.class, TestConfiguration.class})
@TestPropertySource("classpath:application.properties")
public class RatioPerformanceProcessorStepTest {
    private static final String TEAM_NAME = UUID.randomUUID().toString();
    private static final double TEAM_SCORE = 4d;
    private static final String EXPECTED_MIN_PERFORMANCE = "200.00%";
    private static final double MIN_SCORE = 2d;
    private static final String EXPECTED_MAX_PERFORMANCE = "80.00%";
    private static final double MAX_SCORE = 5d;

    @Autowired
    @Qualifier("maxRatioPerformanceProcessor")
    private ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor;

    @Autowired
    @Qualifier("minRatioPerformanceProcessor")
    private ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor;

    @Test
    public void testProcessorUsingSharedStepExecution() throws Exception {
        AverageScoredTeam team = new AverageScoredTeam(TEAM_NAME, TEAM_SCORE);
        TeamPerformance minPerformance = minRatioPerformanceProcessor.process(team);
        assertNotNull(minPerformance);
        assertEquals(minPerformance.getName(), TEAM_NAME);
        assertEquals(minPerformance.getPerformance(), EXPECTED_MIN_PERFORMANCE);

        TeamPerformance maxPerformance = maxRatioPerformanceProcessor.process(team);
        assertNotNull(maxPerformance);
        assertEquals(maxPerformance.getName(), TEAM_NAME);
        assertEquals(maxPerformance.getPerformance(), EXPECTED_MAX_PERFORMANCE);
    }

    public StepExecution getStepExecution() {
        StepExecution stepExecution = mock(StepExecution.class);
        JobExecution jobExecution = mock(JobExecution.class);
        when(stepExecution.getJobExecution()).thenReturn(jobExecution);

        ExecutionContext context = new ExecutionContext();
        context.putDouble(TeamAverageProcessor.MIN_SCORE, MIN_SCORE);
        context.putDouble(TeamAverageProcessor.MAX_SCORE, MAX_SCORE);
        when(jobExecution.getExecutionContext()).thenReturn(context);

        return stepExecution;
    }

    @Test
    void testProcessorsUsingCustomStepExecution() throws Exception {
        StepExecution stepExecution = mock(StepExecution.class);
        JobExecution jobExecution = mock(JobExecution.class);
        when(stepExecution.getJobExecution()).thenReturn(jobExecution);

        ExecutionContext context = new ExecutionContext();
        context.putDouble(TeamAverageProcessor.MIN_SCORE, 1d);
        context.putDouble(TeamAverageProcessor.MAX_SCORE, 2d);
        when(jobExecution.getExecutionContext()).thenReturn(context);

        AverageScoredTeam team = new AverageScoredTeam(TEAM_NAME, 4d);

        TeamPerformance minPerformance = StepScopeTestUtils
                .doInStepScope(stepExecution, () -> minRatioPerformanceProcessor.process(team));
        assertNotNull(minPerformance);
        assertEquals(minPerformance.getName(), TEAM_NAME);
        assertEquals(minPerformance.getPerformance(), "400.00%");

        TeamPerformance maxPerformance = StepScopeTestUtils
                .doInStepScope(stepExecution, () -> maxRatioPerformanceProcessor.process(team));
        assertNotNull(maxPerformance);
        assertEquals(maxPerformance.getName(), TEAM_NAME);
        assertEquals(maxPerformance.getPerformance(), "200.00%");
    }
}
