package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.name.Name;

import java.util.List;

public class FlexibleDatasetSchema extends RelationType<FlexibleDatasetSchema.TableField> {

    SchemaElementDescription description;

    protected FlexibleDatasetSchema(List<TableField> fields) {
        super(fields);
    }

    public static class AbstractField implements ai.dataeng.sqml.schema2.Field {

        Name name;
        SchemaElementDescription description;
        Object default_value;
        //TODO: removed, previous

        @Override
        public Name getName() {
            return name;
        }

    }

    public static class TableField extends AbstractField {

        boolean isPartialSchema;


    }

    public static class FlexibleField extends AbstractField {

        List<FieldType> types;

    }

    public static abstract class FieldType {

        Name variantName;

        Type type;
        int arrayDepth;

        List<Constraint> constraints;
        boolean nonNull;

    }



}
