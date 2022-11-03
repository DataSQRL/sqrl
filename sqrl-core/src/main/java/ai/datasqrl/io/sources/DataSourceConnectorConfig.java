package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.impl.file.DirectorySourceConfig;
import ai.datasqrl.io.impl.file.DirectorySource;
import ai.datasqrl.io.impl.kafka.KafkaSource;
import ai.datasqrl.io.impl.kafka.KafkaSourceConfig;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.NonNull;

import java.io.Serializable;

/**
 * The configuration of a data source that DataSQRL can connect to for data access
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "sourceType")
@JsonSubTypes({@JsonSubTypes.Type(value = DirectorySource.Connector.class, name = DirectorySourceConfig.SOURCE_TYPE),
    @JsonSubTypes.Type(value = KafkaSource.Connector.class, name = KafkaSourceConfig.SOURCE_TYPE),})
public interface DataSourceConnectorConfig extends Serializable {

  DataSourceConnector initialize(@NonNull ErrorCollector errors);

}
