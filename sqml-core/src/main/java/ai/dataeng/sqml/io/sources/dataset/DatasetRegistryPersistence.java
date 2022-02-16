package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.execution.flink.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.tree.name.Name;

import java.io.Closeable;
import java.util.Set;

public interface DatasetRegistryPersistence extends Closeable {

    Set<DataSourceConfiguration> getDatasets();

    void putDataset(DataSourceConfiguration datasource);

    Set<SourceTableConfiguration> getTables(DataSourceConfiguration source);

    void putTable(Name dataset, SourceTableConfiguration table);

    SourceTableStatistics getTableStatistics(Name datasetName, Name tableName);

    void putTableStatistics(Name datasetName, Name tableName, SourceTableStatistics stats);


}
