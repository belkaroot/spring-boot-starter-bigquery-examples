package com.belkatechnologies.bqstarterexamples.manager;

import com.belkatechnologies.bigquery.manager.BigQueryManager;
import com.belkatechnologies.bigquery.manager.FieldValueListDecorator;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Service
/**
 * Examples of data BqManager use cases , for more see java-doc in com.belkatechnologies.bigquery.manager.BigQueryManager
 */
public class BqManagerExamples {

    private final BigQueryManager bigQueryManager;


    public List<Entity> getSomeDataByQueryAndAddToListByConsumer() {
        String query = "SELECT * FROM project.dataset.entity_table where type = @type;";
        Map<String, QueryParameterValue> namedParameters = Map.of("type", QueryParameterValue.string("client"));
        List<Entity> result = new ArrayList<>();
        bigQueryManager.query(query, namedParameters, fvld ->
                result.add(new Entity(
                        fvld.getString("row1"),
                        fvld.getInt("row2"),
                        fvld.getTimestampInstant("row3")
                )));
        return result;
    }

    public List<Entity> getSomeDataByQueryAndAddToListByFunction() {
        String query = "SELECT * FROM project.dataset.entity_table where type = @type;";
        Map<String, QueryParameterValue> namedParameters = Map.of("type", QueryParameterValue.string("client"));
        List<Entity> list = bigQueryManager.list(query, fvld ->
                new Entity(
                        fvld.getString("row1"),
                        fvld.getInt("row2"),
                        fvld.getTimestampInstant("row3")
                ));
        return list;
    }


    void getOne() {
        //way one
        String COUNT = "SELECT count(*) as count FROM dataset.users_table where id is not null and user_id is not null ";
        long rowCounts = bigQueryManager.one(COUNT, values -> values.getLong("count"));

        //way two
        TableResult tableResult = bigQueryManager.query(COUNT);
        if (tableResult.getTotalRows() == 0) {
            //throw or log or whatever
        }
        FieldValue singleValue = FieldValueListDecorator.getSingleValue(tableResult);
        int value = singleValue.getNumericValue().intValue();
    }

    record Entity(String row1, int row2, Instant row3) {
    }
}
