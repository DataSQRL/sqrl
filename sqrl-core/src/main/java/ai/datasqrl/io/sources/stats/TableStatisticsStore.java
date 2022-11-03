package ai.datasqrl.io.sources.stats;

import ai.datasqrl.parse.tree.name.NamePath;

import java.io.Closeable;

public interface TableStatisticsStore extends Closeable {

    void putTableStatistics(NamePath path, SourceTableStatistics stats);

    SourceTableStatistics getTableStatistics(NamePath path);

}
