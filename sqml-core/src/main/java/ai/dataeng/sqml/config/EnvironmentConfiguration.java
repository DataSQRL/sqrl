package ai.dataeng.sqml.config;

import lombok.*;
import org.apache.commons.lang3.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class EnvironmentConfiguration {

    @Builder.Default @NonNull
    @NotNull @Valid
    MetaData metastore = new MetaData();
    @Builder.Default
    boolean monitorSources = true;

    public MetaData getMetastore() {
        return metastore;
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MetaData {

        private static final String DEFAULT_DATABASE = "datasqrl";

        @Builder.Default @NonNull
        @NotNull
        String database = DEFAULT_DATABASE;

        public String getDatabase() {
            return database;
        }


    }


}
