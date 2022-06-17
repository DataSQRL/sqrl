package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.config.provider.TableStatisticsStoreProvider;
import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.execute.StreamHolder;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.util.StreamInputPreparer;
import lombok.AllArgsConstructor;

/**
 * TODO: restart monitoring jobs on minor failures, make sure this is resilient
 */
@AllArgsConstructor
public class SourceTableMonitorImpl implements SourceTableMonitor {

  private final StreamEngine stream;
  private final TableStatisticsStoreProvider.Encapsulated statsStore;
  private final StreamInputPreparer streamPreparer;

  @Override
  public void startTableMonitoring(SourceTable table) {
    //TODO: check if this table is already being monitored
    //Only monitor tables with flexible schemas (i.e. schemas that aren't defined ahead of time)
    if (streamPreparer.isRawInput(table)) {
      StreamEngine.Builder streamBuilder = stream.createJob();
      StreamHolder<SourceRecord.Raw> stream = streamPreparer.getRawInput(table,streamBuilder);
      stream = streamBuilder.monitor(stream, table, statsStore);
      stream.printSink();
      StreamEngine.Job job = streamBuilder.build();
      job.execute(table.qualifiedName());
    } //else do nothing
  }

  @Override
  public void stopTableMonitoring(SourceTable table) {
    //TODO: Look up job by qualified table name and stop if it exists
    throw new UnsupportedOperationException("not yet implemented");
//        Optional<? extends StreamEngine.Job> job = stream.getJob(id);
//        if (job.isPresent()) job.get().cancel();
  }
}
