package ai.dataeng.sqml.config;

import ai.dataeng.sqml.config.engines.FlinkConfiguration;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.io.sources.impl.file.FileSource;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

@Builder
@Getter
public class GlobalConfiguration {

    EnvironmentConfiguration environment;
    @NotNull Engines engines;
    Sources sources;

    public EnvironmentConfiguration getEnvironment() {
        return environment!=null?environment: EnvironmentConfiguration.builder().build();
    }

    @Builder
    @Getter
    public static class Engines {

        @NotNull JDBCConfiguration jdbc;
        FlinkConfiguration flink = FlinkConfiguration.builder().build();



    }

    @Builder
    @Getter
    public static class Sources {

        List<FileSource> directory;

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
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<GlobalConfiguration>> violations = validator.validate(config);
        if (!violations.isEmpty()) {
            for (ConstraintViolation<GlobalConfiguration> violation : violations) {
                System.err.println(violation.getMessage());
            }
            throw new IllegalArgumentException("Invalid configuration - see printed error messages");
        }
        return config;
    }

}
