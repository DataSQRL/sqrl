package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import lombok.NonNull;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface DataSourceConfiguration extends Serializable {

    /**
     * Validates the configuration and initializes the configured {@link DataSource}
     *
     * @return the configured {@link DataSource} or null if invalid
     */
    @Nullable DataSource initialize(ProcessMessage.ProcessBundle<ConfigurationError> errors);


}
