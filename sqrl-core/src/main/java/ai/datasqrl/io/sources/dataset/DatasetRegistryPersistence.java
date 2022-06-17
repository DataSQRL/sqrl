package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.io.sources.DataSource;
import ai.datasqrl.io.sources.SourceTableConfiguration;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;
import java.util.Collection;

public interface DatasetRegistryPersistence {

  Collection<DataSource> getDatasets();

  void putDataset(Name dataset, DataSource datasource);

  boolean removeDataset(Name dataset);

  Collection<SourceTableConfiguration> getTables(Name datasetName);

  void putTable(Name dataset, Name tblName, SourceTableConfiguration table);

  boolean removeTable(Name dataset, Name tblName);

  SourceTableStatistics getTableStatistics(Name datasetName, Name tableName);

  boolean removeTableStatistics(Name datasetName, Name tableName);

}
