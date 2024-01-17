package com.belkatechnologies.bqstarterexamples.streaming;

import com.belkatechnologies.bigquery.configuration.BigQueryProperties;
import com.belkatechnologies.bigquery.manager.BigQueryManager;
import com.belkatechnologies.bigquery.streaming.StreamingManager;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.TableName;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@RequiredArgsConstructor
@Service
public class DummyStreamingService {

    //inject streaming manager
    private final StreamingManager streamingManager;
    //inject bigQueryManager for sql based operations
    private final BigQueryManager bigQueryManager;
    //inject BigQuery object bean configured automatically
    private final BigQuery bigQuery;
    //inject properties you define in application.yaml
    private final BigQueryProperties bigQueryProperties;

    @PostConstruct
    public void streamSomeData() {
        //defining table fields
        Field[] tableFields = new Field[]{
                Field.of("user_id", StandardSQLTypeName.STRING),
                Field.of("note", StandardSQLTypeName.STRING),
                Field.of("timestamp", StandardSQLTypeName.TIMESTAMP)
        };
        //set up schema and definition
        Schema schema = Schema.of(tableFields);
        StandardTableDefinition definition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        //create test table we are going streaming to
        bigQuery.create(TableInfo.of(TableId.of("examples", "user_notes"), definition));

        TableName streamingTableName = TableName.of(bigQueryProperties.getData().getProject(), "examples", "user_notes");
        //stream 1000 rows to the same StreamProcessor instance we created in PostConstruct
        for (int i = 0; i < 1000; i++) {
            //map representing 1 row
            Map<String, Object> oneRow = Map.of("user_id", UUID.randomUUID().toString(),
                    "note", "some_note",
                    "timestamp", System.currentTimeMillis() * 1000);//make sure timestamp in micro second!
            //put row to processor by tableName, streaming processor is created and managed automatically
            streamingManager.putRowForTable(streamingTableName, oneRow);
        }
        //just in case of test demonstration, to have data streamed immediately instead of defined async Streaming Delay
        streamingManager.forceFlushAll();

        //make sure all 1000 rows saved
        String countQuery = "SELECT count(*) as count FROM examples.user_notes ";
        long rowCounts = bigQueryManager.one(countQuery, values -> values.getLong("count"));
        log.info("rowCounts: {}", rowCounts); //1000 rows

        //delete example table
        bigQueryManager.dropTable("examples", "user_notes");
    }
}
