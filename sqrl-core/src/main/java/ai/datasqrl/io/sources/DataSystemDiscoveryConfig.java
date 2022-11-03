package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.impl.file.DirectoryDataSystem;
import ai.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import ai.datasqrl.io.impl.kafka.KafkaDataSystem;
import ai.datasqrl.io.impl.kafka.KafkaDataSystemConfig;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import lombok.NonNull;

import java.io.Serializable;

//@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "sourceType")
@JsonSubTypes({@JsonSubTypes.Type(value = DirectoryDataSystem.Discovery.class, name = DirectoryDataSystemConfig.SOURCE_TYPE),
        @JsonSubTypes.Type(value = KafkaDataSystem.Discovery.class, name = KafkaDataSystemConfig.SOURCE_TYPE),})
public interface DataSystemDiscoveryConfig extends Serializable {

    DataSystemDiscovery initialize(@NonNull ErrorCollector errors);

}
