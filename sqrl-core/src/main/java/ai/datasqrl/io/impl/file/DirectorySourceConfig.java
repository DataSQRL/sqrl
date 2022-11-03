package ai.datasqrl.io.impl.file;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.DataSourceConnector;
import ai.datasqrl.io.sources.DataSourceConnectorConfig;
import ai.datasqrl.io.sources.DataSourceDiscovery;
import ai.datasqrl.io.sources.DataSourceDiscoveryConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.util.regex.Pattern;

@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class DirectorySourceConfig {

    public static final String SOURCE_TYPE = "dir";
    public static final String DEFAULT_PATTERN = "_(\\d+)";

    @NonNull @NotNull
    @Size(min = 3)
    String uri;

    @Builder.Default
    @NonNull @NotNull
    String partPattern = DEFAULT_PATTERN;

    protected boolean rootInitialize(@NonNull ErrorCollector errors) {
        FilePath directoryPath = new FilePath(uri);
        try {
            FilePath.Status status = directoryPath.getStatus();
            if (!status.exists() || !status.isDir()) {
                errors.fatal("URI [%s] is not a directory", uri);
                return false;
            }
        } catch (IOException e) {
            errors.fatal("URI [%s] is invalid: %s", uri, e);
            return false;
        }
        return true;
    }

    public String getSourceType() {
        return SOURCE_TYPE;
    }

    @JsonIgnore
    protected FilePath getPath() {
        return new FilePath(uri);
    }

    @JsonIgnore
    protected Pattern getPattern() {
        return Pattern.compile(partPattern + "$");
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class Connector extends DirectorySourceConfig implements DataSourceConnectorConfig {

        private Connector(Discovery discovery) {
            super(discovery.uri, discovery.partPattern);
        }

        @Override
        public DataSourceConnector initialize(@NonNull ErrorCollector errors) {
            if (rootInitialize(errors)) {
                return new DirectorySource.Connector(getPath(), getPattern());
            } else return null;
        }
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class Discovery extends DirectorySourceConfig implements DataSourceDiscoveryConfig {

        @Override
        public DataSourceDiscovery initialize(@NonNull ErrorCollector errors) {
            if (rootInitialize(errors)) {
                return new DirectorySource.Discovery(getPath(), getPattern(), new Connector(this));
            } else return null;
        }
    }

}
