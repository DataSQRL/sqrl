/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.stats;

import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;

import java.io.Closeable;
import java.util.Map;

public interface TableStatisticsStore extends Closeable {

  void putTableStatistics(NamePath path, SourceTableStatistics stats);

  SourceTableStatistics getTableStatistics(NamePath path);

  Map<Name, SourceTableStatistics> getTablesStatistics(NamePath basePath);

}
