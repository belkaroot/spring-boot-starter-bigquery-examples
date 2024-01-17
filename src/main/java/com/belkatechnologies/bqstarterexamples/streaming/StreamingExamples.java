package com.belkatechnologies.bqstarterexamples.streaming;

import com.belkatechnologies.bigquery.configuration.BigQueryProperties;
import com.belkatechnologies.bigquery.streaming.StreamingManager;
import com.belkatechnologies.bigquery.streaming.processor.BigQueryStreamProcessor;
import com.google.cloud.bigquery.storage.v1.TableName;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@RequiredArgsConstructor
@Service
/**
 * Examples of data streaming in managed and unmanaged ways, for additional features check StreamingManager java-doc
 */
public class StreamingExamples {

    private final StreamingManager streamingManager;
    private final BigQueryProperties properties;

    @PostConstruct
    public void init() {
        //creating a StreamProcessor for permanent use
        //see application.yaml to adjust streaming interval and pollSize
        TableName myTable = TableName.of("project", "dataset", "my_table");
        streamingManager.createStreamProcessor(myTable);
    }

    public void streamBatchByExistingManagedStreamProcessor() {
        streamingManager.putBatchForTable(
                TableName.of("project", "dataset", "my_table"),
                //list representing multiple rows row
                getBatchedData()
        );
    }

    public void streamRowsByExistingManagedStreamProcessor() {
        for (int i = 0; i < 1000; i++) {
            //stream 1000 rows to the same StreamProcessor instance we created in PostConstruct
            streamingManager.putRowForTable(
                    TableName.of("project", "dataset", "my_table"),
                    //map representing 1 row
                    Map.of("user_id", "blabla" + System.currentTimeMillis(),
                            "note", String.valueOf(ThreadLocalRandom.current().nextGaussian()))
            );
        }
    }

    public void streamRowsByNewManagedStreamProcessor() {
        for (int i = 0; i < 1000; i++) {
            //stream 1000 rows to the new StreamProcessor instance, created once at 1st call and then reused
            streamingManager.putRowForTable(
                    TableName.of("project", "dataset", "my_another_table"),
                    //map representing 1 row
                    Map.of("user_id", "blabla" + System.currentTimeMillis(),
                            "note", String.valueOf(ThreadLocalRandom.current().nextGaussian()))
            );
        }
    }

    //in most ways supposed to be used as one time object
    //unmanaged by streamingManager, resources must be managed manually
    //forceFlush can be omitted so data will be flushed in AutoClosable.close()
    public void streamByRowsByStandaloneStreamProcessor() {
        List<Map<String, Object>> data = getBatchedData();
        //should be handled in try-with-recourses
        try (var standaloneStreamProcessor = streamingManager.getStandaloneStreamProcessor(TableName.of("project", "dataset", "my_table_for_standalone_stream"))) {
            standaloneStreamProcessor.putBatch(data);
            standaloneStreamProcessor.forceFlush();
        } catch (Exception e) {
            log.error("Cant stream data", e);
        }
    }

    //in most ways supposed to be used as one time object
    //unmanaged by streamingManager, but resources are managed
    public void sdf() {
        List<Map<String, Object>> data = getBatchedData();
        streamingManager.executeOnceOnStandalone(
                TableName.of("project", "dataset", "my_table_for_standalone_stream"),
                processor -> processor.putBatch(data)
        );
    }



    //list representing multiple rows row
    private List<Map<String, Object>> getBatchedData() {
        return List.of(
                Map.of("user_id", "blabla" + System.currentTimeMillis(), "note", String.valueOf(ThreadLocalRandom.current().nextGaussian())),
                Map.of("user_id", "blabla1" + System.currentTimeMillis(), "note", String.valueOf(ThreadLocalRandom.current().nextGaussian())),
                Map.of("user_id", "blabla2" + System.currentTimeMillis(), "note", String.valueOf(ThreadLocalRandom.current().nextGaussian())),
                Map.of("user_id", "blabla3" + System.currentTimeMillis(), "note", String.valueOf(ThreadLocalRandom.current().nextGaussian())),
                Map.of("user_id", "blabla4" + System.currentTimeMillis(), "note", String.valueOf(ThreadLocalRandom.current().nextGaussian())),
                Map.of("user_id", "blabla5" + System.currentTimeMillis(), "note", String.valueOf(ThreadLocalRandom.current().nextGaussian()))
        );
    }
}
