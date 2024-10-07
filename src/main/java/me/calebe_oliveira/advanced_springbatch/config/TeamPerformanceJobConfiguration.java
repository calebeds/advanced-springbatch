package me.calebe_oliveira.advanced_springbatch.config;

import me.calebe_oliveira.advanced_springbatch.model.AverageScoredTeam;
import me.calebe_oliveira.advanced_springbatch.model.Team;
import me.calebe_oliveira.advanced_springbatch.model.TeamPerformance;
import me.calebe_oliveira.advanced_springbatch.processor.TeamAverageProcessor;
import me.calebe_oliveira.advanced_springbatch.readers.DivisionFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.CommandRunner;
import org.springframework.batch.core.step.tasklet.JvmCommandRunner;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Configuration
@EnableBatchProcessing
public class TeamPerformanceJobConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(TeamPerformanceJobConfiguration.class);
    public static final String SCORE_RANK_PARAM = "scoreRank";
    public static final String UUID_PARAM = "uuid";

    @Value("classpath:input/*.txt")
    private Resource[] inDivisionResources;

    @Value("file:calculated/avg.txt")
    private WritableResource outAvgResource;

    @Value("file:calculated/max.txt")
    private WritableResource maxPerformanceRatioOutResource;

    @Value("file:calculated/min.txt")
    private WritableResource minPerformanceRatioOutResource;

    @Value("file:calculated")
    private WritableResource calculatedDirectoryResource;

    @Bean
    @Qualifier("teamPerformanceJob")
    public Job teamPerformanceJob(JobRepository jobRepository,
                                  @Qualifier("threadPoolTaskExecutor") TaskExecutor threadPoolTaskExecutor,
                                  @Qualifier("averageTeamScoreStep") Step averageTeamScoreStep,
                                  @Qualifier("teamMaxRatioPerformanceStep") Step teamMaxRatioPerformanceStep,
                                  @Qualifier("teamMinRatioPerformanceStep") Step teamMinRatioPerformanceStep,
                                  @Qualifier("shellScriptStep") Step shellScriptStep,
                                  @Qualifier("successLoggerStep") Step successLoggerStep) {
        Flow maxRatioPerformanceFlow = new FlowBuilder<SimpleFlow>("maxRatioPerformanceFlow")
                .start(teamMaxRatioPerformanceStep).build();
        Flow minRatioPerformanceFlow = new FlowBuilder<SimpleFlow>("minRatioPerformanceFlow")
                .start(teamMinRatioPerformanceStep).build();

        Flow performanceSplitFlow = new FlowBuilder<SimpleFlow>("performanceSplitFlow")
                .split(threadPoolTaskExecutor)
                .add(maxRatioPerformanceFlow, minRatioPerformanceFlow)
                .build();

        return new JobBuilder("teamPerformanceJob", jobRepository)
                .start(new FlowBuilder<SimpleFlow>("averageTeamScoreFlow")
                        .start(averageTeamScoreStep)
                        .build())
                .next(performanceSplitFlow)
                .next(shellScriptStep)
                .next(successLoggerStep)
                .build()
                .build();
    }

    @Bean
    @Qualifier("threadPoolTaskExecutor")
    public TaskExecutor threadPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(2);
        return threadPoolTaskExecutor;
    }

    @Bean
    @Qualifier("averageTeamScoreStep")
    public Step averageTeamScoreStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                     @Qualifier("divisionTeamReader") ItemReader<Team> divisionTeamReader,
                                     @Qualifier("teamAverageProcessor") TeamAverageProcessor teamAverageProcessor,
                                     @Qualifier("teamAverageContextPromotionListener") ExecutionContextPromotionListener teamAverageContextPromotionListener,
                                     @Qualifier("jobStartLoggerListener") StepExecutionListener jobStartLoggerListener) {
        return new StepBuilder("averageTeamScoreStep", jobRepository)
                .<Team, AverageScoredTeam>chunk(1, transactionManager)
                .reader(divisionTeamReader)
                .processor(teamAverageProcessor)
                .writer(new FlatFileItemWriterBuilder<AverageScoredTeam>()
                        .name("averageTeamScoreWriter")
                        .resource(outAvgResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(team -> new Object[] {team.getName(), team.getAverageScore()})
                        .build())
                .listener(jobStartLoggerListener)
                .listener(teamAverageContextPromotionListener)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        teamAverageProcessor.setStepExecution(stepExecution);
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        teamAverageProcessor.setStepExecution(null);
                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .faultTolerant()
                .skip(IndexOutOfBoundsException.class)
                .skipLimit(40)
                .listener(new SkipListener<Team, AverageScoredTeam>() {
                    @Override
                    public void onSkipInProcess(Team team, Throwable t) {
                        LOGGER.error("Error while processing team " + team.getName() + ", item is skipped");
                        LOGGER.error("Reason: " + t.getClass().getName() + " -> " + t.getLocalizedMessage());
                    }
                })
                .build();
    }

    @Bean
    @Qualifier("divisionTeamReader")
    public ItemReader<Team> divisionTeamReader() {
        FlatFileItemReader<String> lineReader = new FlatFileItemReaderBuilder<String>()
                .name("divisionLineReader")
                .lineMapper((line, lineNumber) -> line)
                .build();

        DivisionFileReader singleFileMultiLineReader = new DivisionFileReader(lineReader);

        return new MultiResourceItemReaderBuilder<Team>()
                .name("divisionTeamReader")
                .delegate(singleFileMultiLineReader)
                .resources(inDivisionResources)
                .build();
    }


    @Bean
    @StepScope
    @Qualifier("teamAverageProcessor")
    public TeamAverageProcessor teamAverageProcessor(@Value("#{jobParameters['scoreRank']}") int scoreRank) {
        return new TeamAverageProcessor(scoreRank);
    }

    @Bean
    @StepScope
    @Qualifier("teamAverageContextPromotionListener")
    public ExecutionContextPromotionListener teamAverageContextPromotionListener() {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[] {
                TeamAverageProcessor.MAX_SCORE, TeamAverageProcessor.MAX_PLAYER,
                TeamAverageProcessor.MIN_SCORE, TeamAverageProcessor.MIN_PLAYER
        });

        return listener;
    }

    @Bean
    @Qualifier("jobStartLoggerListener")
    public StepExecutionListener jobStartExecutionListener(@Value("#{jobParameters['uuid']}") String uuid) {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                LOGGER.info("Job with uuid = " + uuid + " is started");
            }
        };
    }

    @Bean
    @Qualifier("teamMaxRatioPerformanceStep")
    public Step teamMaxRatioPerformanceStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                            @Qualifier("maxRatioPerformanceProcessor")ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor,
                                            @Qualifier("maxHeaderWriter") FlatFileHeaderCallback maxHeaderWriter) {
        return new StepBuilder("teamMaxRatioPerformanceStep", jobRepository)
                .<AverageScoredTeam, TeamPerformance>chunk(1, transactionManager)
                .reader(averageScoredTeamReader())
                .processor(maxRatioPerformanceProcessor)
                .writer(new FlatFileItemWriterBuilder<TeamPerformance>()
                        .resource(maxPerformanceRatioOutResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(team -> new Object[] {team.getName(), team.getPerformance()})
                        .headerCallback(maxHeaderWriter)
                        .build())
                .build();
    }

    @Bean
    @Qualifier("teamMinRatioPerformanceStep")
    public Step teamMinRatioPerformanceStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                            @Qualifier("minRatioPerformanceProcessor") ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor,
                                            @Qualifier("minHeaderWriter") FlatFileHeaderCallback minHeaderWriter) {
        return new StepBuilder("teamMinRatioPerformanceStep", jobRepository)
                .<AverageScoredTeam, TeamPerformance>chunk(1, transactionManager)
                .reader(averageScoredTeamReader())
                .processor(minRatioPerformanceProcessor)
                .writer(new FlatFileItemWriterBuilder<TeamPerformance>()
                        .resource(maxPerformanceRatioOutResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(team -> new Object[] {team.getName(), team.getPerformance()})
                        .headerCallback(minHeaderWriter)
                        .build())
                .build();
    }

    public ItemReader<AverageScoredTeam> averageScoredTeamReader() {
        return new FlatFileItemReaderBuilder<AverageScoredTeam>()
                .name("averageScoredTeamReader")
                .resource(outAvgResource)
                .lineTokenizer(new DelimitedLineTokenizer(","))
                .fieldSetMapper(fieldSet ->
                        new AverageScoredTeam(fieldSet.readString(0), fieldSet.readDouble(1)))
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("maxRatioPerformanceProcessor")
    public ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor(@Value("#{jobExecutionContext['max.score']}") double maxScore) {
        return item -> process(item, maxScore);
    }

    @Bean
    @StepScope
    @Qualifier("minRatioPerformanceProcessor")
    public ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor(@Value("#{jobExecutionContext['min.score']}") double minScore) {
        return item -> process(item, minScore);
    }

    private static TeamPerformance process(AverageScoredTeam team, double baselineScore) {
        BigDecimal performance = BigDecimal.valueOf(team.getAverageScore())
                .multiply(new BigDecimal(100))
                .divide(BigDecimal.valueOf(baselineScore), 2, RoundingMode.HALF_UP);
        return new TeamPerformance(team.getName(), performance + "%");
    }

    @Bean
    @StepScope
    @Qualifier("maxHeaderWriter")
    public FlatFileHeaderCallback maxHeaderWriter(@Value("#{jobExecutionContext['max.score']}") double maxScore,
                                                  @Value("#{jobExecutionContext['max.player']}") String maxPlayerName) {
        return writer -> writeHeader(writer, maxPlayerName, maxScore);
    }

    @Bean
    @StepScope
    @Qualifier("minHeaderWriter")
    public FlatFileHeaderCallback minHeaderWriter(@Value("#{jobExecutionContext['min.score']}") double minScore,
                                                  @Value("#{jobExecutionContext['min.player']}") String minPlayerName) {
        return writer -> writeHeader(writer, minPlayerName, minScore);
    }

    private static void writeHeader(Writer writer, String name, double score) {
        try {
            writer.write("***********************************************\n");
            writer.write("Team performance below are calculated against" + score + " which was scored by  " +  name + "\n");
            writer.write("***********************************************\n");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    @StepScope
    @Qualifier("shellScriptTasklet")
    public Tasklet shellScriptTasklet(@Value("#{jobParameters['uuid']}") String uuid) {
        return ((contribution, chunkContext) -> {
            CommandRunner commandRunner = new JvmCommandRunner();
            commandRunner.exec(new String[] {"bash", "-l", "-c", "touch" + uuid + ".resulted"},
                    new String[] {}, calculatedDirectoryResource.getFile());
            return RepeatStatus.FINISHED;
        });
    }

    @Bean
    @Qualifier("successLoggerStep")
    public Step successLoggerStep(PlatformTransactionManager transactionManager,
                                  JobRepository jobRepository,
                                  @Qualifier("successLoggerTasklet") Tasklet successLoggerTasklet) {
        return new StepBuilder("successLoggerStep", jobRepository)
                .tasklet(successLoggerTasklet, transactionManager)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("successLoggerTasklet")
    public Tasklet successLoggerTasklet(@Value("#{jobParameters['uuid']}") String uuid) {
        return ((contribution, chunkContext) -> {
            LOGGER.info("Job with uuid = " + uuid + " is finished");
            return RepeatStatus.FINISHED;
        });
    }

    @Bean
    public DataSource dataSource(@Value("${db.url}") String url,
                                 @Value("${db.username}") String username,
                                 @Value("${db.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceScriptDatabaseInitializer(DataSource dataSource,
                                                                                             BatchProperties properties) {
        return new BatchDataSourceScriptDatabaseInitializer(dataSource, properties.getJdbc());
    }

    @Bean
    public BatchProperties batchProperties(@Value("${batch.db.initialize-schema}")DatabaseInitializationMode initializationMode) {
        BatchProperties properties = new BatchProperties();
        properties.getJdbc().setInitializeSchema(initializationMode);
        return properties;
    }
}
