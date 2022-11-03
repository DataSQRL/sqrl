package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import ai.datasqrl.io.impl.kafka.KafkaDataSystemConfig;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.NonNull;

import java.io.Serializable;

/**
 * The configuration of a data source that DataSQRL can connect to for data access
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "sourceType")
@JsonSubTypes({@JsonSubTypes.Type(value = DirectoryDataSystemConfig.Connector.class, name = DirectoryDataSystemConfig.SOURCE_TYPE),
    @JsonSubTypes.Type(value = KafkaDataSystemConfig.Connector.class, name = KafkaDataSystemConfig.SOURCE_TYPE),})
public interface DataSystemConnectorConfig extends Serializable {

  DataSystemConnector initialize(@NonNull ErrorCollector errors);

}
