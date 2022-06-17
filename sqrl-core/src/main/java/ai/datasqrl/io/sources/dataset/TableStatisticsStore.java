package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;

import java.io.Closeable;

public interface TableStatisticsStore extends Closeable {

    void putTableStatistics(Name datasetName, Name tableName, SourceTableStatistics stats);

}
