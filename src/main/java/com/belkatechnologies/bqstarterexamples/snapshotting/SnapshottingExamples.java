package com.belkatechnologies.bqstarterexamples.snapshotting;

import com.belkatechnologies.bigquery.snapshotting.SnapshotConfiguration;
import com.belkatechnologies.bigquery.snapshotting.SnapshotService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
@Service
/**
 * Examples of data Snapshotting use case
 */
public class SnapshottingExamples {

    private final SnapshotService snapshotService;


    /**
     * Scheduled task to create snapshots based on the configured schedule.
     * <p>
     * The task is triggered after an initial delay and runs periodically at a fixed rate.
     *
     * @implNote This method creates snapshots using a predefined configuration, specifying expiration days,
     * waiting for a buffer, and specifying datasets along with associated tables.
     * @see SnapshotConfiguration
     * @see SnapshotService#createSnapshots(SnapshotConfiguration)
     */
    @Scheduled(initialDelay = 2, fixedDelay = 100, timeUnit = TimeUnit.MINUTES)
    public void scheduleCreateSnapshots() {

        // Define the snapshot configuration
        SnapshotConfiguration snapshotConfiguration = SnapshotConfiguration.builder()
                .expirationDays(30)
                .waitBuffer(true)
                .add("bgs_dev", Set.of())
                .add("another_dataset", Set.of())
                .add("one_more_another_dataset", Set.of("table1", "table2"))
                .build();

        // Trigger the creation of snapshots using the configured settings
        snapshotService.createSnapshots(snapshotConfiguration);
    }
}
