package com.datasqrl.packager.config;

import com.datasqrl.spi.GlobalConfiguration;
import com.datasqrl.spi.ManifestConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

import javax.validation.Valid;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class GlobalPackageConfiguration implements GlobalConfiguration {

    public static final String DEPENDENCIES_NAME = "dependencies";

    @JsonProperty("package") @Builder.Default @Valid
    PackageConfiguration pkg = new PackageConfiguration();

    @JsonProperty(DEPENDENCIES_NAME)
    @NonNull @Builder.Default @Valid
    LinkedHashMap<String,Dependency> dependencies = new LinkedHashMap<>();

    @Setter
    @JsonProperty(ManifestConfiguration.PROPERTY)
    ManifestConfiguration manifest;

    public static GlobalPackageConfiguration readFrom(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(path.toFile(), GlobalPackageConfiguration.class);
    }

}
