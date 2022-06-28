package ai.datasqrl.physical.stream;

import ai.datasqrl.config.provider.TableStatisticsStoreProvider;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.util.TimeAnnotatedRecord;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.InputTableSchema;

import java.io.Closeable;
import java.util.Optional;

public interface StreamEngine extends Closeable {

  Builder createJob();

  interface Builder {

    StreamHolder<TimeAnnotatedRecord<String>> fromTextSource(SourceTable table);

    StreamHolder<SourceRecord.Raw> monitor(StreamHolder<SourceRecord.Raw> stream,
                                           SourceTable sourceTable,
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
