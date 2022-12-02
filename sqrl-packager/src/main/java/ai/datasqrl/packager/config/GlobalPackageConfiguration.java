package ai.datasqrl.packager.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

import javax.validation.Valid;
import java.nio.file.Path;
import java.util.LinkedHashMap;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@JsonIgnoreProperties(ignoreUnknown=true)
public class GlobalPackageConfiguration {

    public static final String DEPENDENCIES_NAME = "dependencies";

    @JsonProperty("package") @Builder.Default @Valid
    PackageConfiguration pkg = new PackageConfiguration();

    @JsonProperty(DEPENDENCIES_NAME)
    @NonNull @Builder.Default @Valid
    LinkedHashMap<String,Dependency> dependencies = new LinkedHashMap<>();


    @SneakyThrows
    public static GlobalPackageConfiguration readFrom(Path path) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(path.toFile(), GlobalPackageConfiguration.class);
    }

}
