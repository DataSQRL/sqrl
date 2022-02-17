package ai.dataeng.sqml.config;

import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
public class EnvironmentConfiguration {

    MetaData metastore;
    boolean monitor_sources = true;

    public MetaData getMetastore() {
        return metastore!=null?metastore:MetaData.builder().build();
    }

    @Builder
    @Getter
    public static class MetaData {

        private static final String DEFAULT_DATABASE = "datasqrl";

        String database = DEFAULT_DATABASE;

        public String getDatabase() {
            return database;
        }


    }


}
