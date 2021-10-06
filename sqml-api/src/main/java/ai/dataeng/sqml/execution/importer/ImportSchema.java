package ai.dataeng.sqml.execution.importer;

import ai.dataeng.sqml.ingest.DatasetLookup;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import ai.dataeng.sqml.ingest.source.SourceTable;
import ai.dataeng.sqml.schema2.*;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NamePath;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import java.util.Map;

@AllArgsConstructor
public class ImportSchema {

    private final DatasetLookup datasetLookup;
    private final Map<Name, FlexibleDatasetSchema> sourceSchemas;
    private final RelationType<StandardField> schema;
    private final Map<Name, Mapping> nameMapping;

    public RelationType<StandardField> getSchema() {
        return schema;
    }

    public Mapping getMapping(@NonNull Name name) {
        return nameMapping.get(name);
    }

    public Map<Name, Mapping> getMappings() {
        return nameMapping;
    }

    public SourceTableImport getSourceTable(@NonNull Name tableName) {
        Mapping mapping = nameMapping.get(tableName);
        Preconditions.checkArgument(mapping!=null,"Table has not been imported into local scope: %s", tableName);
        Preconditions.checkArgument(mapping.isSource() && mapping.isTable(), "Name does not reference source table: %s", tableName);
        return getSourceTableInternal(mapping.tableName, mapping.datasetName,
                TypeHelper.getNestedRelation(schema, NamePath.of(tableName)));
    }

    public SourceTableImport getSourceTable(@NonNull Name tableName, @NonNull Name datasetName) {
        Mapping mapping = nameMapping.get(datasetName);
        Preconditions.checkArgument(mapping!=null,"Dataset has not been imported: %s", datasetName);
        Preconditions.checkArgument(mapping.isSource() && mapping.isDataset(), "Name does not reference source dataset: %s", datasetName);
        return getSourceTableInternal(tableName, mapping.datasetName,
                TypeHelper.getNestedRelation(schema, NamePath.of(tableName, datasetName)));
    }

    private SourceTableImport getSourceTableInternal(@NonNull Name originalTableName, @NonNull Name originalDSName,
                                                     @NonNull RelationType<StandardField> tableSchema) {
        SourceDataset ds = datasetLookup.getDataset(originalDSName);
        Preconditions.checkArgument(ds!=null, "Dataset does not exist: %s", originalDSName);
        FlexibleDatasetSchema dsSchema = sourceSchemas.get(originalDSName);
        assert dsSchema!=null;
        SourceTable table = ds.getTable(originalTableName);
        Preconditions.checkArgument(table!=null, "Dataset [%s] does not contain table: %s", originalDSName, originalTableName);
        FlexibleDatasetSchema.TableField sourceSchema = dsSchema.getFieldByName(originalTableName);
        assert sourceSchema!=null;
        return new SourceTableImport(table, sourceSchema, tableSchema);
    }

    enum ImportType {
        SCRIPT, SOURCE;
    }

    @Value
    public static class Mapping {

        @NonNull
        final ImportType type;
        @NonNull
        final Name datasetName;
        final Name tableName;

        public boolean isTable() {
            return tableName!=null;
        }

        public boolean isDataset() {
            return !isTable();
        }

        public boolean isSource() {
            return type==ImportType.SOURCE;
        }

        public boolean isScript() {
            return type==ImportType.SCRIPT;
        }

    }


    @Value
    public static class SourceTableImport {

        @NonNull
        private final SourceTable table;
        @NonNull
        private final FlexibleDatasetSchema.TableField sourceSchema;
        @NonNull
        private final RelationType<StandardField> tableSchema;

    }

}
