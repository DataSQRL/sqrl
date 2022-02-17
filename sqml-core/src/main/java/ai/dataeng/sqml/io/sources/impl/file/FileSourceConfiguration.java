package ai.dataeng.sqml.io.sources.impl.file;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.impl.CanonicalizerConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.h2.util.StringUtils;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.regex.Pattern;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileSourceConfiguration implements DataSourceConfiguration {

    public static final String DEFAULT_PATTERN = "_(\\d+)";

    private String name;
    private CanonicalizerConfiguration canonicalizer;
    @NonNull
    private String path;
    private String pattern;

    private DataSource validateAndInitialize(ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        NameCanonicalizer canon = CanonicalizerConfiguration.get(canonicalizer);

        String locationName = name!=null?name:path;
        Path directoryPath;
        try {
            directoryPath = Path.of(path);
            if (!Files.exists(directoryPath) || !Files.isDirectory(directoryPath)) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,locationName,"Path is invalid: %s", path));
                return null;
            }
            if (!Files.isReadable(directoryPath)) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,locationName,"Directory cannot be read: %s", path));
                return null;
            }
        } catch (InvalidPathException e) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,locationName,"Path is invalid: %s", path));
            return null;
        }

        Name n = Name.of(StringUtils.isNullOrEmpty(name)?directoryPath.getFileName().toString():name,canon);

        Pattern partPattern = Pattern.compile((pattern!=null?pattern:DEFAULT_PATTERN)+"$");
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
