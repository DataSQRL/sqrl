package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.config.error.ErrorCollector;

import java.util.Collection;

import lombok.NonNull;

public interface DataSource {

    /**
     * The name of the dataset produced by this data source.
     * The name must be unique within a server instance.
     *
     * @return name of dataset
     */
    @NonNull Name getDatasetName();

    @NonNull NameCanonicalizer getCanonicalizer();

    Collection<SourceTableConfiguration> discoverTables(ErrorCollector errors);

    boolean update(@NonNull DataSourceConfiguration config, @NonNull ErrorCollector errors);

    DataSourceConfiguration getConfiguration();

}
