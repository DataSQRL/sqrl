package com.datasqrl.physical.stream;

import com.datasqrl.io.sources.stats.TableStatisticsStoreProvider;
import com.datasqrl.io.sources.SourceRecord;
import com.datasqrl.io.sources.dataset.TableInput;
import com.datasqrl.io.sources.util.TimeAnnotatedRecord;
import com.datasqrl.physical.ExecutionEngine;
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

    void addAsTable(StreamHolder<SourceRecord.Named> stream, InputTableSchema schema, String qualifiedTableName);

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
