package ai.datasqrl.config;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

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
