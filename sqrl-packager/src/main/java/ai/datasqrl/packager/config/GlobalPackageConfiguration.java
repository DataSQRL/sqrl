package ai.datasqrl.packager.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

import java.nio.file.Path;
import java.util.LinkedHashMap;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@JsonIgnoreProperties(ignoreUnknown=true)
public class GlobalPackageConfiguration {

    @JsonProperty("package")
    PackageConfiguration pkg;

    LinkedHashMap<String,Dependency> dependencies;


    @SneakyThrows
    public static GlobalPackageConfiguration readFrom(Path path) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(path.toFile(), GlobalPackageConfiguration.class);
    }

}
