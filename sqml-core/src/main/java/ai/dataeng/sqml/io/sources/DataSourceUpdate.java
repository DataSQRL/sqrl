package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.constraints.OptionalMinString;
import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Strings;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DataSourceUpdate {

    @OptionalMinString
    String name;

    @NonNull @NotNull @Valid
    DataSourceImplementation source;

    @Builder.Default @NonNull @NotNull @Valid
    DataSourceConfiguration config = new DataSourceConfiguration();

    /**
     * Whether this datasource should automatically discover available tables
     * when the data source is added and register those tables with the source.
     *
     * If false, tables have to be added explicitly through the configuration.
     */
    @Builder.Default
    boolean discoverTables = true;

    @Valid @Builder.Default @NonNull @NotNull
    List<SourceTableConfiguration> tables = Collections.EMPTY_LIST;

    public boolean initialize(ErrorCollector errors) {
        if (!ConfigurationUtil.javaxValidate(this, errors)) return false;
        if (Strings.isNullOrEmpty(name)) {
            Optional<String> defaultName = source.getDefaultName();
            if (defaultName.isEmpty()) {
                errors.fatal("Source needs to have a name");
                return false;
            }
            name = defaultName.get();
        }
        if (!Name.validName(name)) {
            errors.fatal("Source needs to have valid name, but given: %s", name);
            return false;
        }
        errors = errors.resolve(name);
        if (!source.initialize(errors)) return false;
        if (!config.initialize(errors)) return false;
        //Tables need to be validated once DataSource is established.
        return true;
    }


}
