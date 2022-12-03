package com.datasqrl.spi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import lombok.*;

import javax.validation.constraints.NotEmpty;
import java.util.Optional;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class ManifestConfiguration {

    public static final String PROPERTY = "manifest";

    @NonNull @NotEmpty
    String main;
    String graphql;

    @JsonIgnore
    public Optional<String> getOptGraphQL() {
        if (Strings.isNullOrEmpty(graphql)) return  Optional.empty();
        else return Optional.of(graphql);
    }

}
