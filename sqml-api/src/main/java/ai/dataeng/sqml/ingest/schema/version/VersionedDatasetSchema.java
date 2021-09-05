package ai.dataeng.sqml.ingest.schema.version;

import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SchemaElementDescription;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.name.Name;

import java.util.List;

public class VersionedDatasetSchema extends RelationType<VersionedDatasetSchema.TableField> {

    VersionContainer<SchemaElementDescription> descriptions;

    protected VersionedDatasetSchema(List<TableField> fields) {
        super(fields);
    }

    public static class AbstractField implements ai.dataeng.sqml.schema2.Field {

        VersionContainer<Name> names; //includes removed & previous
        VersionContainer<SchemaElementDescription> descriptions;
        VersionContainer<Object> default_values;

        @Override
        public Name getName() {
            throw new UnsupportedOperationException();
        }

    }

    public static class TableField extends AbstractField {

        VersionContainer<Boolean> isPartialSchema;


    }

    public static class FlexibleField extends AbstractField {

        VersionContainer<List<FlexibleDatasetSchema.FieldType>> types;

    }

}
