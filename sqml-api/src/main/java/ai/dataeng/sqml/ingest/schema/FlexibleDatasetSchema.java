package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.schema.version.VersionIdentifier;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.name.Name;
import lombok.*;

import java.util.List;

@Getter
public class FlexibleDatasetSchema extends RelationType<FlexibleDatasetSchema.TableField> {

    private final SchemaElementDescription description;
    private final VersionIdentifier versionId;

    private FlexibleDatasetSchema(@NonNull List<TableField> fields, @NonNull SchemaElementDescription description, @NonNull VersionIdentifier versionId) {
        super(fields);
        this.description = description;
        this.versionId = versionId;
    }

    @Setter
    public static class Builder extends RelationType.AbstractBuilder<FlexibleDatasetSchema.TableField, Builder> {

        private SchemaElementDescription description;
        private VersionIdentifier versionId;

        public Builder() {
            super(true);
        }

        public FlexibleDatasetSchema build() {
            return new FlexibleDatasetSchema(fields, description, versionId);
        }

    }

    @Getter
    @ToString
    @AllArgsConstructor
    public static class AbstractField implements ai.dataeng.sqml.schema2.Field {

        private final Name name;
        private final SchemaElementDescription description;
        private final Object default_value;
        private final NameMapping nameMapping;

        @Setter
        public static abstract class Builder {

            protected Name name;
            protected SchemaElementDescription description;
            protected Object default_value;
            protected NameMapping nameMapping;

        }

    }

    @Getter
    @ToString
    public static class TableField extends AbstractField {

        private final boolean isPartialSchema;
        private final RelationType<FlexibleField> fields;
        private final List<Constraint> constraints;

        public TableField(Name name, SchemaElementDescription description, Object default_value, NameMapping nameMapping,
                          boolean isPartialSchema, RelationType<FlexibleField> fields, List<Constraint> constraints) {
            super(name,description,default_value,nameMapping);
            this.isPartialSchema = isPartialSchema;
            this.fields = fields;
            this.constraints = constraints;
        }

        @Setter
        public static class Builder extends AbstractField.Builder {

            protected boolean isPartialSchema;
            protected RelationType<FlexibleField> fields;
            protected List<Constraint> constraints;

            public TableField build() {
                return new TableField(name,description,default_value, nameMapping, isPartialSchema, fields, constraints);
            }

        }
    }

    @Getter
    @ToString
    public static class FlexibleField extends AbstractField implements Field {

        private final List<FieldType> types;

        public FlexibleField(Name name, SchemaElementDescription description, Object default_value, NameMapping nameMapping,
                             List<FieldType> types) {
            super(name, description, default_value, nameMapping);
            this.types = types;
        }

        @Setter
        public static class Builder extends AbstractField.Builder {

            protected List<FieldType> types;

            public FlexibleField build() {
                return new FlexibleField(name,description,default_value, nameMapping, types);
            }

        }
    }

    @Value
    public static class FieldType {

        private final Name variantName;

        private final Type type;
        private final int arrayDepth;

        private final List<Constraint> constraints;

    }



}
