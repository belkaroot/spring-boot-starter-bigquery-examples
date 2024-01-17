package com.belkatechnologies.bqstarterexamples.streaming.hook;

import com.belkatechnologies.bigquery.streaming.hook.StreamFailedHook;
import com.belkatechnologies.bigquery.streaming.processor.StreamingObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CustomStreamFailedHook implements StreamFailedHook {

    @Override
    public void onStreamFail(Exception batch, StreamingObject streamingObject) {
        //logs, your metrics, notifications, custom save logic, etc
        log.info("onStreamFail called");
    }
}
