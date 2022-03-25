package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.io.sources.impl.file.FilePath;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class JsonLineFormat implements TextLineFormat<JsonLineFormat.Configuration> {

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

    public static class JsonLineParser implements TextLineFormat.Parser {

        private transient ObjectMapper mapper;

        public JsonLineParser() {}


        private ObjectMapper getMapper() {
            if (mapper==null) {
                mapper = new ObjectMapper();
            }
            return mapper;
        }

        @Override
        public Result parse(@NonNull String line) {
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
    public static class Configuration implements FormatConfiguration {

        @Override
        public boolean validate(ProcessMessage.ProcessBundle<ConfigurationError> errors) {
            return true;
        }
    }

}
