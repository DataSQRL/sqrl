package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.constraints.OptionalMinString;
import ai.dataeng.sqml.io.sources.impl.InputPreview;
import ai.dataeng.sqml.tree.name.Name;

import ai.dataeng.sqml.io.sources.formats.*;

import ai.dataeng.sqml.config.error.ErrorCollector;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.*;

import javax.validation.Valid;
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
                              @NonNull String format) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(format));
        Preconditions.checkNotNull(!Strings.isNullOrEmpty(name));
        this.name = name;
        this.identifier = name;
        this.format = format;
        this.formatConfig = null;
    }

    public boolean validateAndInitialize(DataSource source, ErrorCollector errors) {
        if (!Name.validName(name)) {
            errors.fatal("Table needs to have valid name: %s",name);
            return false;
        }
        errors = errors.resolve(name);
        if (Strings.isNullOrEmpty(identifier)) identifier = name;
        identifier = source.getCanonicalizer().getCanonical(identifier);
        if (!Strings.isNullOrEmpty(format)) format = format.trim().toLowerCase();
        if (!FileFormat.validFormat(format)) {
            errors.fatal("Table has invalid format: %s", format);
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
            errors.fatal("Table does not have format configuration and it cannot be inferred");
            return false;
        } else {
            if (formatConfig.validate(errors.resolve("format"))) {
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

    public boolean update(@NonNull SourceTableConfiguration config, @NonNull ErrorCollector errors) {
        //TODO: implement
        return false;
    }

    @Value
    public static class Named {

        private final Name name;
        private final SourceTableConfiguration tableConfig;

    }

}