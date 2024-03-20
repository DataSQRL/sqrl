package com.datasqrl.metadata;

import com.datasqrl.metadata.stats.SourceTableStatistics;
import com.datasqrl.canonicalizer.NamePath;
import java.io.IOException;
import java.io.Serializable;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MetricStoreProvider implements MetricStore.Provider<SourceTableStatistics>,
    Serializable {

  private final MetadataStoreProvider storeProvider;
  private final NamePath path;

  @Override
  public MetricStore<SourceTableStatistics> open() {
    return new Store(getStatsStore(storeProvider), path);
  }

  public static TableStatisticsStore getStatsStore(MetadataStoreProvider storeProvider) {
    return new TableStatisticsStore(new MetadataNamePathPersistence(
        storeProvider.openStore()));
  }

  @AllArgsConstructor
  static class Store implements MetricStore<SourceTableStatistics> {

    private final TableStatisticsStore store;
    private final NamePath path;

    @Override
    public void put(SourceTableStatistics metric) {
      store.putTableStatistics(path, metric);
    }

    @Override
    public void close() throws IOException {
      store.close();
    }
  }

}
