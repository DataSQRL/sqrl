package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.sources.stats.SourceTableStatistics;
import ai.dataeng.sqml.tree.name.Name;
import java.util.Set;

public interface DatasetRegistryPersistence {

    Set<DataSourceConfiguration> getDatasets();

    void putDataset(Name dataset, DataSourceConfiguration datasource);

    Set<SourceTableConfiguration> getTables(Name datasetName);

    void putTable(Name dataset, Name tblName, SourceTableConfiguration table);

    SourceTableStatistics getTableStatistics(Name datasetName, Name tableName);

    void putTableStatistics(Name datasetName, Name tableName, SourceTableStatistics stats);


}
