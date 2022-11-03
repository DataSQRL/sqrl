package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.impl.file.DirectorySource;
import ai.datasqrl.io.impl.file.DirectorySourceConfig;
import ai.datasqrl.io.impl.kafka.KafkaSource;
import ai.datasqrl.io.impl.kafka.KafkaSourceConfig;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.NonNull;

import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "sourceType")
@JsonSubTypes({@JsonSubTypes.Type(value = DirectorySource.Discovery.class, name = DirectorySourceConfig.SOURCE_TYPE),
        @JsonSubTypes.Type(value = KafkaSource.Discovery.class, name = KafkaSourceConfig.SOURCE_TYPE),})
public interface DataSourceDiscoveryConfig extends Serializable {

    DataSourceDiscovery initialize(@NonNull ErrorCollector errors);

}
