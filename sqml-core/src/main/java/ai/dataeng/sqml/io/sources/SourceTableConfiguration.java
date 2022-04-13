package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.constraints.OptionalMinString;
import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.io.impl.InputPreview;
import ai.dataeng.sqml.tree.name.Name;

import ai.dataeng.sqml.io.formats.*;

import ai.dataeng.sqml.config.error.ErrorCollector;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Builder
@ToString
@Getter
public class SourceTableConfiguration implements Serializable {

    @NonNull @NotNull @Size(min=3)
    String name;
    @OptionalMinString
    String identifier;
    @Valid
    FormatConfiguration format;

    public SourceTableConfiguration(@NonNull String name,
                              @NonNull FormatConfiguration format) {
        this.name = name;
        this.identifier = name;
        this.format = format;
    }

    public SourceTableConfiguration(@NonNull String name) {

    }

    public boolean validateAndInitialize(DataSource source, ErrorCollector errors) {
        if (!ConfigurationUtil.javaxValidate(this, errors)) return false;
        if (!Name.validName(name)) {
            errors.fatal("Table needs to have valid name: %s",name);
            return false;
        }
        errors = errors.resolve(name);
        if (Strings.isNullOrEmpty(identifier)) identifier = name;
        identifier = source.getCanonicalizer().getCanonical(identifier);

        if (format==null && source.config.getFormat() == null) {
            errors.fatal("Need to specify a table format");
            return false;
        } else if (format == null) {
            format = source.config.getFormat();
        }

        return format.initialize(new InputPreview(source,this), errors.resolve("format"));
    }

    @JsonIgnore
    public Format.Parser getFormatParser() {
        return format.getImplementation().getParser(format);
    }

    @JsonIgnore
    public FileFormat getFileFormat() {
        return format.getFileFormat();
    }

    public boolean update(@NonNull SourceTableConfiguration config, @NonNull ErrorCollector errors) {
        //TODO: implement
        return false;
    }

}