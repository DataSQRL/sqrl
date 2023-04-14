/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery.store;

import com.datasqrl.io.stats.SourceTableStatistics;
import com.datasqrl.metadata.MetadataNamePathPersistence;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor
public class TableStatisticsStore implements Closeable {

  public static final String STORE_TABLE_STATS_KEY = "stats";

  private final MetadataNamePathPersistence store;

  public void putTableStatistics(NamePath path, SourceTableStatistics stats) {
    store.put(stats, path, STORE_TABLE_STATS_KEY);
  }

  public SourceTableStatistics getTableStatistics(NamePath path) {
    return store.get(SourceTableStatistics.class, path, STORE_TABLE_STATS_KEY);
  }

  public Map<Name, SourceTableStatistics> getTablesStatistics(NamePath basePath) {
    return store.getSubPaths(basePath).stream()
        .map(path -> Pair.of(path.getLast(), getTableStatistics(path))).
        filter(pair -> pair.getValue() != null)
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  @Override
  public void close() throws IOException {
    store.close();
  }

}
