package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.config.error.ErrorCollector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
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

        public JsonLineParser() {
            mapper = new ObjectMapper();
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
    @JsonSerialize
    public static class Configuration implements FormatConfiguration {



        @Override
        public boolean validate(ErrorCollector errors) {
            return true;
        }
    }

}
