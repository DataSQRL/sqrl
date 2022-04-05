package ai.dataeng.sqml.io.impl.file;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.io.sinks.DataSinkConfiguration;
import ai.dataeng.sqml.io.impl.file.FilePath;
import ai.dataeng.sqml.io.impl.file.FileSourceConfiguration;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.IOException;

@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FileSinkConfiguration implements DataSinkConfiguration {

    public static final String DEFAULT_PART_DELIMITER = "_p";

    @NonNull @NotNull @Size(min=3)
    String uri;
    @Builder.Default @NonNull @NotNull @Size(min=1)
    String partDelimiter = DEFAULT_PART_DELIMITER;
    @Builder.Default @NonNull @NotNull
    String charset = FileSourceConfiguration.DEFAULT_CHARSET;


    @Override
    public boolean validateAndInitialize(ErrorCollector errors) {
        if (!ConfigurationUtil.javaxValidate(this, errors)) return false;

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

        if (Strings.isNullOrEmpty(partDelimiter)) {
            errors.fatal("Part delimiter can not be empty");
        }
        return true;
    }
}
