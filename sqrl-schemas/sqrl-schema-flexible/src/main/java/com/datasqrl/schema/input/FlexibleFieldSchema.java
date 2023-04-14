package com.datasqrl.schema.input;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.type.Type;
import lombok.*;

import java.io.Serializable;
import java.util.List;

@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public abstract class FlexibleFieldSchema implements SchemaField {

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

        public void copyFrom(FlexibleFieldSchema f) {
            name = f.name;
            description = f.description;
            default_value = f.default_value;
        }

    }


    @Getter
    @ToString(callSuper = true)
    @NoArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class Field extends FlexibleFieldSchema implements SchemaField {

        @NonNull
        private List<FieldType> types;

        public Field(Name name, SchemaElementDescription description, Object default_value,
                     List<FieldType> types) {
            super(name, description, default_value);
            this.types = types;
        }

        @Setter
        public static class Builder extends FlexibleFieldSchema.Builder {

            protected List<FieldType> types;

            public void copyFrom(Field f) {
                super.copyFrom(f);
                types = f.types;
            }

            public Field build() {
                return new Field(name, description, default_value, types);
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
