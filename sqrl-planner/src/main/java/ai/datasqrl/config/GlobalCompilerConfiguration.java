package ai.datasqrl.config;


import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.util.ConfigurationUtil;
import ai.datasqrl.spi.ManifestConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.Valid;
import java.io.IOException;
import java.nio.file.Path;

@SuperBuilder
@NoArgsConstructor
@Getter
public class GlobalCompilerConfiguration extends GlobalEngineConfiguration {

    @Builder.Default @NonNull @Valid
    CompilerConfiguration compiler = new CompilerConfiguration();

    @Valid
    @JsonProperty(ManifestConfiguration.PROPERTY)
    ManifestConfiguration manifest;

    public CompilerConfiguration initializeCompiler(ErrorCollector errors) {
        if (!ConfigurationUtil.javaxValidate(this,errors)) return null;
        return compiler;
    }

    public static GlobalCompilerConfiguration readFrom(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(path.toFile(), GlobalCompilerConfiguration.class);
    }

}
