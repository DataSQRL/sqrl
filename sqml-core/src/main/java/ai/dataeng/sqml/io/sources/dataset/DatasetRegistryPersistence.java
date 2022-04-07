package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.sources.stats.SourceTableStatistics;
import ai.dataeng.sqml.tree.name.Name;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

public interface DatasetRegistryPersistence {

    Collection<DataSource> getDatasets();

    void putDataset(Name dataset, DataSource datasource);

    boolean removeDataset(Name dataset);

    Collection<SourceTableConfiguration> getTables(Name datasetName);

    void putTable(Name dataset, Name tblName, SourceTableConfiguration table);

    boolean removeTable(Name dataset, Name tblName);

    SourceTableStatistics getTableStatistics(Name datasetName, Name tableName);

    void putTableStatistics(Name datasetName, Name tableName, SourceTableStatistics stats);

    boolean removeTableStatistics(Name datasetName, Name tableName);

}
