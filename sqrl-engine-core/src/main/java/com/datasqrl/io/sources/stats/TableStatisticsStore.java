package com.datasqrl.io.sources.stats;

import com.datasqrl.parse.tree.name.Name;
import com.datasqrl.parse.tree.name.NamePath;

import java.io.Closeable;
import java.util.Map;

public interface TableStatisticsStore extends Closeable {

    void putTableStatistics(NamePath path, SourceTableStatistics stats);

    SourceTableStatistics getTableStatistics(NamePath path);

    Map<Name,SourceTableStatistics> getTablesStatistics(NamePath basePath);

}
