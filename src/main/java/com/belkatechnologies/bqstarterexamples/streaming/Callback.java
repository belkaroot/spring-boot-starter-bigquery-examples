package com.belkatechnologies.bqstarterexamples.streaming;

import com.belkatechnologies.bigquery.streaming.callback.DefaultAbstractAppendCompleteCallback;
import com.belkatechnologies.bigquery.streaming.processor.BigQueryStreamProcessor;
import com.belkatechnologies.bigquery.streaming.processor.StreamingObject;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
public class Callback extends DefaultAbstractAppendCompleteCallback {

    //constructor must be the same as parent
    public Callback(BigQueryStreamProcessor parent, StreamingObject batch, Phaser phaser, AtomicLong processedRows, AtomicLong processedBytes) {
        super(parent, batch, phaser, processedRows, processedBytes);
    }

    @Override
    public void doOnSuccess(AppendRowsResponse appendRowsResponse) {
        log.info("doOnSuccess custom logic");
    }

    @Override
    public void doOnSuccessButHasError(AppendRowsResponse appendRowsResponse) {
        log.info("doOnSuccessButHasError custom logic");
    }

    @Override
    public void doOnFailure(Throwable throwable) {
        log.info("doOnFailure custom logic");
    }
}
