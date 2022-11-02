package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.config.constraints.OptionalMinString;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.util.ConfigurationUtil;
import ai.datasqrl.io.SharedConfiguration;
import ai.datasqrl.io.sources.DataSystemConnector;
import ai.datasqrl.io.sources.DataSystemConnectorConfig;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.Serializable;

@SuperBuilder
@NoArgsConstructor
@Getter
public class TableConfig extends SharedConfiguration implements Serializable {

    @NonNull @NotNull
    @Size(min = 3)
    String name;
    @OptionalMinString
    String identifier;
    @Valid @NonNull @NotNull
    DataSystemConnectorConfig datasource;

    /**
     * TODO: make this configurable
     * @return
     */
    @JsonIgnore
    public SchemaAdjustmentSettings getSchemaAdjustmentSettings() {
        return SchemaAdjustmentSettings.DEFAULT;
    }

    private DataSystemConnector baseInitialize(ErrorCollector errors, NamePath basePath) {
        if (!Name.validName(name)) {
            errors.fatal("Table needs to have valid name: %s", name);
            return null;
        }
        errors = errors.resolve(name);
        if (!rootInitialize(errors,true)) return null;
        if (!ConfigurationUtil.javaxValidate(this, errors)) {
            return null;
        }

        if (Strings.isNullOrEmpty(identifier)) {
            identifier = name;
        }
        identifier = getCanonicalizer().getCanonicalizer().getCanonical(identifier);

        if (!format.initialize(errors.resolve("format"))) return null;

        return datasource.initialize(errors.resolve(name).resolve("datasource"));

    }

    public TableSource initializeSource(ErrorCollector errors, NamePath basePath,
                                        FlexibleDatasetSchema.TableField schema) {
        DataSystemConnector connector = baseInitialize(errors,basePath);
        if (connector==null) return null;
        Name tableName = getName();
        return new TableSource(connector,this,basePath.concat(tableName), tableName, schema);
    }

    public TableSink initializeSink(ErrorCollector errors, NamePath basePath) {
        DataSystemConnector connector = baseInitialize(errors,basePath);
        if (connector==null) return null;
        Name tableName = getName();
        return new TableSink(connector, this, basePath.concat(tableName), tableName);
    }

    @JsonIgnore
    public Name getName() {
        return Name.of(name,getCanonicalizer().getCanonicalizer());
    }

    public static TableConfigBuilder copy(SharedConfiguration config) {
        return TableConfig.builder()
                .canonicalizer(config.getCanonicalizer())
                .charset(config.getCharset())
                .format(config.getFormat());
    }

}
