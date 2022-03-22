package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import java.io.Serializable;
import javax.annotation.Nullable;

public interface DataSourceConfiguration extends Serializable {

    /**
     * Validates the configuration and initializes the configured {@link DataSource}
     *
     * @return the configured {@link DataSource} or null if invalid
     */
    @Nullable DataSource initialize(ProcessMessage.ProcessBundle<ConfigurationError> errors);


}
