package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.config.constraints.OptionalMinString;
import ai.dataeng.sqml.io.sources.impl.InputPreview;
import ai.dataeng.sqml.tree.name.Name;
import java.io.Serializable;
import java.util.Optional;

import ai.dataeng.sqml.io.sources.formats.*;

import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@NoArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public class SourceTableConfiguration {

    @NonNull @NotNull @Size(min=3)
    String name;
    @OptionalMinString
    String identifier;
    @NonNull @NotNull @Size(min=2)
    String format;
    @Valid FormatConfiguration formatConfig;

    public SourceTableConfiguration(@NonNull String name,
                              @NonNull String format, @NonNull FormatConfiguration formatConfig) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(format));
        Preconditions.checkNotNull(!Strings.isNullOrEmpty(name));
        this.name = name;
        this.identifier = name;
        this.format = format;
        this.formatConfig = formatConfig;
    }

    public boolean validateAndInitialize(DataSource source, ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        Name dataset = source.getDatasetName();
        if (Strings.isNullOrEmpty(name)) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,dataset,"Table needs to have valid name"));
            return false;
        }
        if (Strings.isNullOrEmpty(identifier)) identifier = name;
        if (!Strings.isNullOrEmpty(format)) format = format.trim().toLowerCase();
        if (FileFormat.validFormat(format)) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,dataset,"Table [%s] needs to have valid name",name));
            return false;
        }
        Format<FormatConfiguration> formatImpl = getFormatImpl();
        if (formatConfig == null) {
            //Try to infer it
            Format.ConfigurationInference<FormatConfiguration> inferer = formatImpl.getConfigInferer().orElse(null);
            if (inferer != null) {
                InputPreview preview = new InputPreview(source,this);
                FormatConfigInferer<FormatConfiguration> fci = new FormatConfigInferer<>(inferer,preview);
                formatConfig = fci.inferConfig().orElse(null);
            }
        }
        if (formatConfig == null) {
            //Try default
            formatConfig = formatImpl.getDefaultConfiguration().orElse(null);
        }
        if (formatConfig == null) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,dataset,
                    "Table [%s] does not have format configuration and it cannot be inferred", name));
            return false;
        } else {
            if (formatConfig.validate(errors)) {
                return true;
            } else {
                return false;
            }
        }
    }

    private Format<FormatConfiguration> getFormatImpl() {
        return FileFormat.getFormat(format).getImplementation();
    }

    public Format.Parser getFormatParser() {
        Format<FormatConfiguration> formatImpl = getFormatImpl();
        return formatImpl.getParser(formatConfig);
    }

    public boolean update(@NonNull SourceTableConfiguration config, @NonNull ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        //TODO: implement
        return false;
    }

    @Value
    public static class Named {

        private final Name name;
        private final SourceTableConfiguration tableConfig;

    }

}