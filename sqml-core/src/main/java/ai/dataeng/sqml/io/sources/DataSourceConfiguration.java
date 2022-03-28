package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.error.ErrorCollector;

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
    @Nullable DataSource initialize(ErrorCollector errors);


}
