package ai.datasqrl.physical.stream.flink.monitor;

import ai.datasqrl.config.provider.*;
import ai.datasqrl.io.sources.dataset.AbstractExternalTable;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.TableStatisticsStore;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


public class SaveTableStatistics extends RichSinkFunction<SourceTableStatistics> {

  private final TableStatisticsStoreProvider.Encapsulated statisticsStore;
  private final SourceTable.Digest tableDigest;

  private transient TableStatisticsStore store;

  public SaveTableStatistics(TableStatisticsStoreProvider.Encapsulated statisticsStore, AbstractExternalTable.Digest tableDigest) {
    this.statisticsStore = statisticsStore;
    this.tableDigest = tableDigest;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    store = statisticsStore.openStore();
  }

  @Override
  public void close() throws Exception {
    store.close();
    store = null;
  }

  @Override
  public void invoke(SourceTableStatistics stats, Context context) throws Exception {
    store.putTableStatistics(tableDigest.getPath(), stats);
  }

}
