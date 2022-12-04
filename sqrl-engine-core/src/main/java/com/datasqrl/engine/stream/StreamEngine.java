/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream;

import com.datasqrl.io.stats.TableStatisticsStoreProvider;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.schema.input.InputTableSchema;

import java.io.Closeable;
import java.util.Optional;

public interface StreamEngine extends Closeable, ExecutionEngine {

  Builder createJob();

  interface Builder {

    StreamHolder<TimeAnnotatedRecord<String>> fromTextSource(TableInput table);

    StreamHolder<SourceRecord.Raw> monitor(StreamHolder<SourceRecord.Raw> stream,
        TableInput tableSource,
        TableStatisticsStoreProvider.Encapsulated statisticsStoreProvider);

    void addAsTable(StreamHolder<SourceRecord.Named> stream, InputTableSchema schema,
        String qualifiedTableName);

    Job build();

  }

  Optional<? extends Job> getJob(String id);

  interface Job {

    String getId();

    void execute(String name);

    void cancel();

    Status getStatus();

    enum Status {PREPARING, RUNNING, COMPLETED, FAILED}

  }

}
