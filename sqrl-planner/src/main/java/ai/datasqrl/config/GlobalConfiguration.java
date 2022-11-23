package ai.datasqrl.config;

import ai.datasqrl.config.engines.*;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.util.ConfigurationUtil;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class GlobalConfiguration {

  @Builder.Default
  @NonNull
  @NotNull @Valid
  DiscoveryConfiguration discovery = new DiscoveryConfiguration();
  @NonNull
  @NotNull @Valid
  Engines engines;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Engines {

    //Databases
    @Valid
    JDBCConfiguration jdbc;
    @Valid
    InMemoryDatabaseConfiguration inmemoryDB;
    @Valid
    FileDatabaseConfiguration fileDB;
    //Streams
    @Valid
    FlinkConfiguration flink;
    @Valid
    InMemoryStreamConfiguration inmemoryStream;


  }

  public static GlobalConfiguration fromFile(@NonNull Path ymlFile) {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    GlobalConfiguration config;
    try {
      config = mapper.readValue(Files.readString(ymlFile),
          GlobalConfiguration.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Could not parse configuration [" + ymlFile + "]", e);
    } catch (IOException e) {
      throw new IllegalArgumentException("Could read configuration file [" + ymlFile + "]", e);
    }
    return config;
  }

  public boolean validate(ErrorCollector errors) {
    return ConfigurationUtil.javaxValidate(this, errors);
  }

}
