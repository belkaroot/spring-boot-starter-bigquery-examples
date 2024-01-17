package com.belkatechnologies.bqstarterexamples.streaming.hook;

import com.belkatechnologies.bigquery.streaming.hook.PreAppendHook;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CustomPreAppendHook implements PreAppendHook {

    @Override
    public void preAppendAction(String table, JSONArray jsonArray) {
        //logs, your metrics, notifications, custom save logic, etc
        log.info("preAppendAction called");
    }
}
