package ai.datasqrl.config;

import ai.datasqrl.config.engines.FlinkConfiguration;
import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.scripts.FileScriptConfiguration;
import ai.datasqrl.config.util.ConfigurationUtil;
import ai.datasqrl.io.sinks.DataSinkRegistration;
import ai.datasqrl.io.sources.DataSourceUpdate;
import ai.datasqrl.config.error.ErrorCollector;

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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class GlobalConfiguration {

    @Builder.Default @NonNull
    @NotNull @Valid
    EnvironmentConfiguration environment = new EnvironmentConfiguration();
    @NonNull
    @NotNull @Valid
    Engines engines;
    @Builder.Default @NonNull
    @NotNull @Valid
    List<DataSourceUpdate> sources = Collections.EMPTY_LIST;
    @Builder.Default @NonNull
    @NotNull @Valid
    List<DataSinkRegistration> sinks = Collections.EMPTY_LIST;
    @Builder.Default @NonNull
    @NotNull @Valid
    List<FileScriptConfiguration> scripts = Collections.EMPTY_LIST;

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Engines {

        @NonNull
        @NotNull @Valid
        JDBCConfiguration jdbc;
        @Valid
        FlinkConfiguration flink;



    }

    public static GlobalConfiguration fromFile(@NonNull Path ymlFile) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        GlobalConfiguration config;
        try {
            config = mapper.readValue(Files.readString(ymlFile),
                    GlobalConfiguration.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not parse configuration ["+ymlFile+"]",e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Could read configuration file ["+ymlFile+"]",e);
        }
        return config;
    }

    public ErrorCollector validate() {
        return ConfigurationUtil.javaxValidate(this);
    }

}
