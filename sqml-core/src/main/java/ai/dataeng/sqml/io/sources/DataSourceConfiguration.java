package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface DataSourceConfiguration extends Serializable {

    /**
     *
     * @return true if successfully validated and initialized, else false
     */
    boolean validate(ProcessMessage.ProcessBundle<ConfigurationError> errors);

    DataSource initialize();




}
