package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.io.impl.file.FileSourceConfiguration;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import javax.annotation.Nullable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "sourceType")
@JsonSubTypes({ @JsonSubTypes.Type(value = FileSourceConfiguration.class, name = "file"), })
public interface DataSourceConfiguration extends Serializable {

    /**
     * Validates the configuration and initializes the configured {@link DataSource}
     *
     * @return the configured {@link DataSource} or null if invalid
     */
    @Nullable DataSource initialize(String name, ErrorCollector errors);


}
