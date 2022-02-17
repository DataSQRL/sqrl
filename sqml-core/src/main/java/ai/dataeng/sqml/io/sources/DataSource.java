package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import lombok.NonNull;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public interface DataSource {

    /**
     * The name of the dataset produced by this data source.
     * The name must be unique within a server instance.
     *
     * @return name of dataset
     */
    @NonNull Name getDatasetName();

    @NonNull NameCanonicalizer getCanonicalizer();

    Collection<? extends SourceTableConfiguration> pollTables(long maxWait, TimeUnit timeUnit) throws InterruptedException;

    boolean isCompatible(@NonNull DataSource other);

    DataSourceConfiguration getConfiguration();

}
