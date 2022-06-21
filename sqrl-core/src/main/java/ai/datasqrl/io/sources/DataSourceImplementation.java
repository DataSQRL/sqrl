package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.io.impl.kafka.KafkaSourceImplementation;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;
import lombok.NonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "sourceType")
@JsonSubTypes({@JsonSubTypes.Type(value = DirectorySourceImplementation.class, name = "dir"),
    @JsonSubTypes.Type(value = KafkaSourceImplementation.class, name = "kafka"),})
public interface DataSourceImplementation extends Serializable {

  boolean initialize(@NonNull ErrorCollector errors);

  /**
   * The name of the dataset produced by this data source. The name must be unique within a server
   * instance.
   *
   * @return name of dataset
   */
  @NonNull Optional<String> getDefaultName();

  public boolean hasSourceTimestamp();

//    @NonNull NameCanonicalizer getCanonicalizer();

  Collection<SourceTableConfiguration> discoverTables(@NonNull DataSourceConfiguration config,
      @NonNull ErrorCollector errors);

  boolean update(@NonNull DataSourceConfiguration config, @NonNull ErrorCollector errors);

//    DataSourceConfiguration getConfiguration();

}
