package ai.dataeng.sqml.io.sinks;

import ai.dataeng.sqml.config.constraints.OptionalMinString;
import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.io.formats.FileFormat;
import ai.dataeng.sqml.io.formats.Format;
import ai.dataeng.sqml.io.formats.FormatConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Strings;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@ToString
public class DataSinkRegistration {

    @NonNull @NotNull @Size(min=3)
    String name;

    @NonNull @NotNull @Valid
    DataSinkConfiguration config;

    @OptionalMinString
    String format;

    @Valid
    FormatConfiguration formatConfig;


    public boolean validateAndInitialize(ErrorCollector errors) {
        if (!ConfigurationUtil.javaxValidate(this, errors)) return false;
        if (!Name.validName(name)) {
            errors.fatal("Sink needs to have valid name: %s",name);
            return false;
        }
        errors = errors.resolve(name);

        if (!config.validateAndInitialize(errors.resolve("config"))) {
            return false;
        }

        if (formatConfig == null) {
            //Try using default format (if exists)
            if (Strings.isNullOrEmpty(format)) {
                errors.fatal("Need to specify a sink format: %s", format);
                return false;
            }
            format = format.trim().toLowerCase();
            if (!FileFormat.validFormat(format)) {
                errors.fatal("Sink has invalid format: %s", format);
                return false;
            }
            Format<FormatConfiguration> formatImpl = FileFormat.getFormat(format).getImplementation();
            formatConfig = formatImpl.getDefaultConfiguration().orElse(null);
        }
        if (formatConfig == null) {
            errors.fatal("Sink does not have format configuration and no default exists");
            return false;
        } else {
            if (formatConfig.validate(errors.resolve("format"))) {
                format = formatConfig.getName();
                return true;
            } else {
                return false;
            }
        }
    }


}
