package ai.datasqrl.config;

import ai.datasqrl.config.engines.*;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.FileScriptConfiguration;
import ai.datasqrl.config.util.ConfigurationUtil;
import ai.datasqrl.io.sinks.DataSinkRegistration;
import ai.datasqrl.io.sources.DataSourceUpdate;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class GlobalConfiguration {

  @Builder.Default
  @NonNull
  @NotNull @Valid
  EnvironmentConfiguration environment = new EnvironmentConfiguration();
  @NonNull
  @NotNull @Valid
  Engines engines;
  @Builder.Default
  @NonNull
  @NotNull @Valid
  List<DataSourceUpdate> sources = Collections.EMPTY_LIST;
  @Builder.Default
  @NonNull
  @NotNull @Valid
  List<DataSinkRegistration> sinks = Collections.EMPTY_LIST;
  @Builder.Default
  @NonNull
  @NotNull @Valid
  List<FileScriptConfiguration> scripts = Collections.EMPTY_LIST;

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
