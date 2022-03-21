package ai.dataeng.sqml.type.schema;

import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.Field;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.constraint.Constraint;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

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
    @NoArgsConstructor
    public static abstract class AbstractField implements ai.dataeng.sqml.type.Field {

        @NonNull
        private Name name;
        @NonNull
        private SchemaElementDescription description;
        private Object default_value;

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

        @Override
        public Field withAlias(QualifiedName alias) {
            return null;
        }
    }

    @Getter
    @ToString
    @NoArgsConstructor
    public static class TableField extends AbstractField {

        private boolean isPartialSchema;
        @NonNull
        private RelationType<FlexibleField> fields;
        @NonNull
        private List<Constraint> constraints;

        public TableField(Name name, SchemaElementDescription description, Object default_value,
                          boolean isPartialSchema, RelationType<FlexibleField> fields, List<Constraint> constraints) {
            super(name,description,default_value);
            this.isPartialSchema = isPartialSchema;
            this.fields = fields;
            this.constraints = constraints;
        }

        @Override
        public Optional<QualifiedName> getAlias() {
            return Optional.empty();
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
    @NoArgsConstructor
    public static class FlexibleField extends AbstractField implements Field {

        @NonNull
        private List<FieldType> types;

        public FlexibleField(Name name, SchemaElementDescription description, Object default_value,
                             List<FieldType> types) {
            super(name, description, default_value);
            this.types = types;
        }

        @Override
        public Optional<QualifiedName> getAlias() {
            return Optional.empty();
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

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    public static class FieldType implements Serializable {

        @NonNull
        private Name variantName;

        @NonNull
        private Type type;
        private int arrayDepth;

        @NonNull
        private List<Constraint> constraints;

    }


}
