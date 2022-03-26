package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import lombok.NonNull;

import java.io.Serializable;
import javax.annotation.Nullable;

public interface DataSourceConfiguration extends Serializable {

    /**
     * Whether this datasource should automatically discover available tables
     * when the data source is added and register those tables with the source.
     *
     * If false, tables have to be added explicitly through the configuration.
     *
     * @return
     */
    boolean discoverTables();

    /**
     * Validates the configuration and initializes the configured {@link DataSource}
     *
     * @return the configured {@link DataSource} or null if invalid
     */
    @Nullable DataSource initialize(ProcessMessage.ProcessBundle<ConfigurationError> errors);


}
