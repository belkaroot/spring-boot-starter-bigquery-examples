package com.belkatechnologies.bqstarterexamples.utils;

import com.belkatechnologies.bigquery.manager.BigQueryManager;
import com.belkatechnologies.bigquery.utils.BatchQueryExecutor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@RequiredArgsConstructor
@Service
/**
 * Examples of Utils package use cases
 */
public class UtilsExamples {

    private final BigQueryManager bigQueryManager;

    //load whole table(selected fields) to memory, see com.belkatechnologies.bigquery.utils.BatchQueryExecutor java doc
    //this approach can be used to load data from bq to any other services such as redis, but if table is huge(>10GB) better user Bigquery Read API
    public void multithreadingLoad() {
        String loadAllQuery = "SELECT id, name, platform, email FROM %s.users where platform is not null and name is not null ";
        String countQuery = "SELECT count(*) as count FROM %s.users where platform is not null and name is not null ";

        Map<String, Map<String, Integer>> map = new ConcurrentHashMap<>();
        BatchQueryExecutor batchQueryExecutor = new BatchQueryExecutor(countQuery, loadAllQuery, bigQueryManager, "users", "id", rs -> {
            int id = rs.getInt("id");
            String name = rs.getString(1);
            String platform = rs.getString(2);
            map.computeIfAbsent(platform, p -> new HashMap<>()).put(name, id);
        });
        batchQueryExecutor.fetch();
    }


}

