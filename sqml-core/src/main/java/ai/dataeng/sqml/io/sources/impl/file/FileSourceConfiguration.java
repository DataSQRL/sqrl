package ai.dataeng.sqml.io.sources.impl.file;

import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.impl.CanonicalizerConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.config.error.ErrorCollector;

import java.io.IOException;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class FileSourceConfiguration implements DataSourceConfiguration {

    public static final String DEFAULT_PATTERN = "_(\\d+)";
    public static final String DEFAULT_CHARSET = "UTF-8";


    @Builder.Default @NonNull @NotNull
    CanonicalizerConfiguration canonicalizer = CanonicalizerConfiguration.system;
    @NonNull @NotNull @Size(min=3)
    String uri;
    @Builder.Default @NonNull @NotNull
    String pattern = DEFAULT_PATTERN;
    @Builder.Default @NonNull @NotNull
    String charset = DEFAULT_CHARSET;
    @Builder.Default
    boolean discoverFiles = true;


    private DataSource validateAndInitialize(String name, ErrorCollector errors) {
        if (!ConfigurationUtil.javaxValidate(this, errors)) return null;
        NameCanonicalizer canon = canonicalizer.getCanonicalizer();
        if (Strings.isNullOrEmpty(name)) {
            name = (new FilePath(uri)).getFileName();
        }
        if (!Name.validName(name)) {
            errors.fatal("Invalid data source name: %s", name);
            return null;
        }

        Name nname = canonicalizer.getCanonicalizer().name(name);
        errors = errors.resolve(nname);
        FilePath directoryPath = new FilePath(uri);
        try {
            FilePath.Status status = directoryPath.getStatus();
            if (!status.exists() || !status.isDir()) {
                errors.fatal("URI [%s] is not a directory", uri);
                return null;
            }
        } catch (IOException e) {
            errors.fatal("URI [%s] is invalid: %s", uri, e);
            return null;
        }

        Pattern partPattern = Pattern.compile(pattern+"$");
        return new FileSource(nname,canon,directoryPath,partPattern,this);
    }

    @Override
    public DataSource initialize(String name, ErrorCollector errors) {
        DataSource source = validateAndInitialize(name, errors);
        return source;
    }
}
