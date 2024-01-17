package com.belkatechnologies.bqstarterexamples.streaming.hook;

import com.belkatechnologies.bigquery.streaming.hook.PostAppendHook;
import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class CustomPostAppendHook implements PostAppendHook {

    @Override
    public void postAppendAction(ApiFuture<AppendRowsResponse> callback, AtomicLong processedRows) {
        //logs, your metrics, notifications, custom save logic, etc
        log.info("postAppendAction called");
    }
}
