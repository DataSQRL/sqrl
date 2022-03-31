package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.config.error.ErrorCollector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class JsonLineFormat implements TextLineFormat<JsonLineFormat.Configuration> {

    public static final FileFormat FORMAT = FileFormat.JSON;
    public static final String NAME = "json";

    @Override
    public Parser getParser(Configuration config) {
        return new JsonLineParser();
    }

    @Override
    public Optional<Configuration> getDefaultConfiguration() {
        return Optional.of(new Configuration());
    }

    @Override
    public Optional<ConfigurationInference<Configuration>> getConfigInferer() {
        return Optional.empty();
    }

    @NoArgsConstructor
    public static class JsonLineParser implements TextLineFormat.Parser {

        private transient ObjectMapper mapper;

        @Override
        public Result parse(@NonNull String line) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                Map<String,Object> record = mapper.readValue(line,Map.class);
                return Result.success(record);
            } catch (IOException e) {
                return Result.error(e.getMessage());
            }
        }
    }


    @NoArgsConstructor
    @ToString
    @Builder
    @Getter
    @JsonSerialize
    public static class Configuration implements FormatConfiguration {

        @Override
        public boolean validate(ErrorCollector errors) {
            return true;
        }

        @Override
        public FileFormat getFileFormat() {
            return FORMAT;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

}
