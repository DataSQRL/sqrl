package ai.datasqrl.physical.stream.flink.monitor;

import ai.datasqrl.config.provider.*;
import ai.datasqrl.io.sources.dataset.TableStatisticsStore;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


public class SaveTableStatistics extends RichSinkFunction<SourceTableStatistics> {

  private final TableStatisticsStoreProvider.Encapsulated statisticsStore;
  private final Name dataset;
  private final Name table;

  private transient TableStatisticsStore store;

  public SaveTableStatistics(TableStatisticsStoreProvider.Encapsulated statisticsStore,
      Name dataset, Name table) {
    this.statisticsStore = statisticsStore;
    this.dataset = dataset;
    this.table = table;
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
    store.putTableStatistics(dataset, table, stats);
  }

}
