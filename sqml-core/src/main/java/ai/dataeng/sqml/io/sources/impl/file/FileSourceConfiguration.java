package ai.dataeng.sqml.io.sources.impl.file;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.impl.CanonicalizerConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.google.common.base.Preconditions;
import lombok.*;
import org.h2.util.StringUtils;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.regex.Pattern;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class FileSourceConfiguration implements DataSourceConfiguration {

    public static final String DEFAULT_PATTERN = "_(\\d+)";

    String name;
    @Builder.Default
    @NonNull CanonicalizerConfiguration canonicalizer = CanonicalizerConfiguration.system;
    @NonNull String uri;
    @Builder.Default
    @NonNull String pattern = DEFAULT_PATTERN;

    private DataSource validateAndInitialize(ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        NameCanonicalizer canon = canonicalizer.getCanonicalizer();

        String locationName = name!=null?name: uri;
        Path directoryPath;
        try {
            directoryPath = Path.of(uri);
            if (!Files.exists(directoryPath) || !Files.isDirectory(directoryPath)) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,locationName,"Path is invalid: %s", uri));
                return null;
            }
            if (!Files.isReadable(directoryPath)) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,locationName,"Directory cannot be read: %s", uri));
                return null;
            }
        } catch (InvalidPathException e) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,locationName,"Path is invalid: %s", uri));
            return null;
        }

        Name n = Name.of(StringUtils.isNullOrEmpty(name)?directoryPath.getFileName().toString():name,canon);

        Pattern partPattern = Pattern.compile(pattern+"$");
        return new FileSource(n,canon,directoryPath,partPattern,this);
    }

    @Override
    public boolean validate(ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        validateAndInitialize(errors);
        return !errors.isFatal();
    }

    @Override
    public DataSource initialize() {
        DataSource source = validateAndInitialize(new ProcessMessage.ProcessBundle<>());
        Preconditions.checkArgument(source!=null,"Invalid configuration for source");
        return source;
    }
}
