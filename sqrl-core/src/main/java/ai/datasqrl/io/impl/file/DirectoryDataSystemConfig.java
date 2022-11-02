package ai.datasqrl.io.impl.file;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.DataSystemConnector;
import ai.datasqrl.io.sources.DataSystemConnectorConfig;
import ai.datasqrl.io.sources.DataSystemDiscovery;
import ai.datasqrl.io.sources.DataSystemDiscoveryConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.nio.file.Path;
import java.util.regex.Pattern;

@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class DirectoryDataSystemConfig {

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
    public static class Connector extends DirectoryDataSystemConfig implements DataSystemConnectorConfig {

        private Connector(Discovery discovery) {
            super(discovery.uri, discovery.partPattern);
        }

        @Override
        public DataSystemConnector initialize(@NonNull ErrorCollector errors) {
            if (rootInitialize(errors)) {
                return new DirectoryDataSystem.Connector(getPath(), getPattern());
            } else return null;
        }
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class Discovery extends DirectoryDataSystemConfig implements DataSystemDiscoveryConfig {

        @Override
        public DataSystemDiscovery initialize(@NonNull ErrorCollector errors) {
            if (rootInitialize(errors)) {
                return new DirectoryDataSystem.Discovery(getPath(), getPattern(), new Connector(this));
            } else return null;
        }

    }

    public static DataSystemDiscoveryConfig of(Path path) {
        return Discovery.builder().uri(path.toUri().getPath()).build();
    }


}
