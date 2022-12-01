package ai.datasqrl.packager.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@JsonIgnoreProperties(ignoreUnknown=true)
public class PackageConfiguration {

    @NonNull @NotNull @NotEmpty
    String name;
    @NonNull @NotNull @NotEmpty
    String version;

    String variant;
    String license;
    String repository;
    String homepage;
    String documentation;
    String readme;
    String description;

}
