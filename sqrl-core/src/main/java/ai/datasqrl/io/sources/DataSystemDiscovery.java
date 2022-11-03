package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.dataset.TableConfig;
import lombok.NonNull;

import java.util.Collection;
import java.util.Optional;

public interface DataSystemDiscovery {

    /**
     * The name of the dataset produced by this data source if discoverable from the configuration
     *
     * @return name of dataset
     */
    @NonNull Optional<String> getDefaultName();

    boolean requiresFormat();

    Collection<TableConfig> discoverTables(@NonNull DataSystemConfig config,
                                           @NonNull ErrorCollector errors);

}
