package ai.dataeng.sqml.config;

import ai.dataeng.sqml.config.engines.FlinkConfiguration;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.config.scripts.FileScriptConfiguration;
import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import lombok.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.validation.*;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
    Sources sources = new Sources();
    @Builder.Default @NonNull
    @NotNull @Valid
    List<FileScriptConfiguration> scripts = new ArrayList<>();

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

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Sources {

        @Builder.Default @NonNull
        @NotNull
        List<FileSourceConfiguration> directory = new ArrayList<>();

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

    public ProcessMessage.ProcessBundle<ConfigurationError> validate() {
        return ConfigurationUtil.javaxValidate(this);
    }

}
