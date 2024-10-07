package me.calebe_oliveira.advanced_springbatch.controllers;

import me.calebe_oliveira.advanced_springbatch.config.TeamPerformanceJobConfiguration;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@EnableScheduling
public class TeamPerformanceController {
    private final JobLauncher jobLauncher;
    private final Job teamPerformanceJob;

    public TeamPerformanceController(@Qualifier("asyncJobLauncher") JobLauncher jobLauncher,
                                     @Qualifier("teamPerformanceJob") Job teamPerformanceJob) {
        this.jobLauncher = jobLauncher;
        this.teamPerformanceJob = teamPerformanceJob;
    }

    @PostMapping("/start")
    public String start(@RequestParam("score") long scoreRank) throws Exception {
        String uuid = UUID.randomUUID().toString();
        launchJobAsynchronously(scoreRank, uuid);
        return "Job with id " + uuid + " was submitted";
    }

//    @Scheduled(cron = "0 0/30 * * * ?") // Uncomment to scheduled behavior
    public void scheduledJobStarter() throws Exception {
        launchJobAsynchronously(0, UUID.randomUUID().toString());
    }


    private void launchJobAsynchronously(long scoreRank, String uuid) throws Exception {
      jobLauncher.run(teamPerformanceJob, new JobParametersBuilder()
              .addLong(TeamPerformanceJobConfiguration.SCORE_RANK_PARAM, scoreRank)
              .addString(TeamPerformanceJobConfiguration.UUID_PARAM, uuid)
              .toJobParameters());
    }
}
