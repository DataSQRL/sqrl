package ai.dataeng.sqml.config;

import ai.dataeng.sqml.config.engines.FlinkConfiguration;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import lombok.NonNull;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

public class GlobalConfiguration {

    public Engines engines;
    public Sources sources;

    public static class Engines {

        public JDBCConfiguration jdbc;
        public FlinkConfiguration flink;


    }

    public static class Sources {

        public List<FileSourceConfiguration> directory;

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
