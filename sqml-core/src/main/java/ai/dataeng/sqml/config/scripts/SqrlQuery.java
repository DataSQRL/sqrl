package ai.dataeng.sqml.config.scripts;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import lombok.*;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.Serializable;

@Value
public class SqrlQuery implements Serializable {

    private final Name name;
    private final String qraphQL;

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Config {

        @NonNull @NotNull @Size(min = 3, max = 128)
        private String name;
        @NonNull @NotNull @Size(min = 10)
        private String qraphQL;

        public SqrlQuery initialize(ProcessMessage.ProcessBundle<ConfigurationError> errors,
                                    String bundleName, NameCanonicalizer canonicalizer) {
            return new SqrlQuery(Name.of(name,canonicalizer),qraphQL);
        }

    }

}
