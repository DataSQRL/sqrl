package ai.dataeng.sqml.config.scripts;

import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.config.error.ErrorCollector;

import java.io.Serializable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

@Value
public class SqrlQuery implements Serializable {

    private final Name name;
    private final String filename;
    private final String qraphQL;

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Config {

        @NonNull @NotNull @Size(min = 3, max = 128)
        private String name;
        private String filename;
        @NonNull @NotNull @Size(min = 10)
        private String qraphQL;

        public SqrlQuery initialize(ErrorCollector errors, NameCanonicalizer canonicalizer) {
            if (!ConfigurationUtil.javaxValidate(this, errors)) return null;

            return new SqrlQuery(Name.of(name,canonicalizer),
                    StringUtils.isNotEmpty(filename)?filename:name,
                    qraphQL);
        }

    }

}
