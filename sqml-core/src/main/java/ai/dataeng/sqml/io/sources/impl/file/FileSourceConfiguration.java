package ai.dataeng.sqml.io.sources.impl.file;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.impl.CanonicalizerConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;

import java.io.IOException;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.h2.util.StringUtils;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class FileSourceConfiguration implements DataSourceConfiguration {

    public static final String DEFAULT_PATTERN = "_(\\d+)";
    public static final String DEFAULT_CHARSET = "UTF-8";

    String name;
    @Builder.Default @NonNull @NotNull
    CanonicalizerConfiguration canonicalizer = CanonicalizerConfiguration.system;
    @NonNull @NotNull @Size(min=3)
    String uri;
    @Builder.Default @NonNull @NotNull
    String pattern = DEFAULT_PATTERN;
    @Builder.Default @NonNull @NotNull
    String charset = DEFAULT_CHARSET;
    @Builder.Default
    boolean discoverTables = true;
    @Builder.Default
    boolean discoverFiles = true;


    private DataSource validateAndInitialize(ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        NameCanonicalizer canon = canonicalizer.getCanonicalizer();
        if (Strings.isNullOrEmpty(name)) {
            name = (new FilePath(uri)).getFileName();
        }
        if (!Name.validName(name)) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,name,"Invalid name: %s", this.name));
            return null;
        }

        Name nname = canonicalizer.getCanonicalizer().name(name);
        FilePath directoryPath = new FilePath(uri);
        try {
            FilePath.Status status = directoryPath.getStatus();
            if (!status.exists() || !status.isDir()) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,nname.getDisplay(),"URI [%s] is not a directory", uri));
                return null;
            }
        } catch (IOException e) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,nname.getDisplay(),"URI [%s] is invalid: %s", uri, e));
            return null;
        }

        Pattern partPattern = Pattern.compile(pattern+"$");
        return new FileSource(nname,canon,directoryPath,partPattern,this);
    }

    @Override
    public boolean discoverTables() {
        return discoverTables;
    }

    @Override
    public DataSource initialize(ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        DataSource source = validateAndInitialize(errors);
        return source;
    }
}
