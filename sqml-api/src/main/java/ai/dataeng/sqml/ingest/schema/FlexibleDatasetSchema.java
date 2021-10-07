package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.name.Name;
import lombok.*;

import java.util.Collections;
import java.util.List;

@Getter
public class FlexibleDatasetSchema extends RelationType<FlexibleDatasetSchema.TableField> {

    public static final FlexibleDatasetSchema EMPTY = new FlexibleDatasetSchema(Collections.EMPTY_LIST, SchemaElementDescription.NONE);

    @NonNull
    private final SchemaElementDescription description;

    private FlexibleDatasetSchema(@NonNull List<TableField> fields, @NonNull SchemaElementDescription description) {
        super(fields);
        this.description = description;
    }

    @Setter
    public static class Builder extends RelationType.AbstractBuilder<FlexibleDatasetSchema.TableField, Builder> {

        private SchemaElementDescription description = SchemaElementDescription.NONE;

        public Builder() {
            super(true);
        }

        public FlexibleDatasetSchema build() {
            return new FlexibleDatasetSchema(fields, description);
        }

    }

    @Getter
    @ToString
    @AllArgsConstructor
    public static abstract class AbstractField implements ai.dataeng.sqml.schema2.Field {

        @NonNull
        private final Name name;
        @NonNull
        private final SchemaElementDescription description;
        private final Object default_value;

        @Setter
        public static abstract class Builder {

            protected Name name;
            protected SchemaElementDescription description = SchemaElementDescription.NONE;
            protected Object default_value;

            public void copyFrom(AbstractField f) {
                name = f.name;
                description = f.description;
                default_value = f.default_value;
            }

        }

    }

    @Getter
    @ToString
    public static class TableField extends AbstractField {

        private final boolean isPartialSchema;
        @NonNull
        private final RelationType<FlexibleField> fields;
        @NonNull
        private final List<Constraint> constraints;

        public TableField(Name name, SchemaElementDescription description, Object default_value,
                          boolean isPartialSchema, RelationType<FlexibleField> fields, List<Constraint> constraints) {
            super(name,description,default_value);
            this.isPartialSchema = isPartialSchema;
            this.fields = fields;
            this.constraints = constraints;
        }

        @Setter
        public static class Builder extends AbstractField.Builder {

            protected boolean isPartialSchema = true;
            protected RelationType<FlexibleField> fields;
            protected List<Constraint> constraints = Collections.EMPTY_LIST;

            public void copyFrom(TableField f) {
                super.copyFrom(f);
                isPartialSchema = f.isPartialSchema;
                fields = f.fields;
                constraints = f.constraints;
            }

            public TableField build() {
                return new TableField(name,description,default_value, isPartialSchema, fields, constraints);
            }

        }

        public static TableField empty(Name name) {
            Builder b = new Builder();
            b.setName(name);
            b.setFields(RelationType.EMPTY);
            return b.build();
        }
    }

    @Getter
    @ToString
    public static class FlexibleField extends AbstractField implements Field {

        @NonNull
        private final List<FieldType> types;

        public FlexibleField(Name name, SchemaElementDescription description, Object default_value,
                             List<FieldType> types) {
            super(name, description, default_value);
            this.types = types;
        }

        @Setter
        public static class Builder extends AbstractField.Builder {

            protected List<FieldType> types;

            public void copyFrom(FlexibleField f) {
                super.copyFrom(f);
                types = f.types;
            }

            public FlexibleField build() {
                return new FlexibleField(name,description,default_value, types);
            }

        }
    }

    @Value
    public static class FieldType {

        @NonNull
        private final Name variantName;

        @NonNull
        private final Type type;
        private final int arrayDepth;

        @NonNull
        private final List<Constraint> constraints;

    }


}
