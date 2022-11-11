package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import ai.datasqrl.io.impl.kafka.KafkaDataSystemConfig;
import ai.datasqrl.io.impl.print.PrintDataSystem;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.NonNull;

import java.io.Serializable;

/**
 * The configuration of a data source that DataSQRL can connect to for data access
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "systemType")
@JsonSubTypes({@JsonSubTypes.Type(value = DirectoryDataSystemConfig.Connector.class, name = DirectoryDataSystemConfig.SYSTEM_TYPE),
    @JsonSubTypes.Type(value = KafkaDataSystemConfig.Connector.class, name = KafkaDataSystemConfig.SYSTEM_TYPE),
        @JsonSubTypes.Type(value = PrintDataSystem.Connector.class, name = PrintDataSystem.SYSTEM_TYPE),})
public interface DataSystemConnectorConfig extends Serializable {

  DataSystemConnector initialize(@NonNull ErrorCollector errors);

}
