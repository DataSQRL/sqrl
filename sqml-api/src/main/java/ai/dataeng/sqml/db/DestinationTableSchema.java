package ai.dataeng.sqml.db;

import ai.dataeng.sqml.ingest.SourceTableSchema;
import ai.dataeng.sqml.type.ScalarType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import lombok.Value;

import java.io.Serializable;
import java.util.*;

@Value
public class DestinationTableSchema implements Serializable, Iterable<DestinationTableSchema.Field> {

    private final Field[] fields;

    private DestinationTableSchema(Field[] fields) {
        this.fields = fields;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Field get(String fieldName) {
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].name.equals(fieldName)) return fields[i];
        }
        return null;
    }

    public Field get(int position) {
        Preconditions.checkArgument(position>=0 && position<fields.length);
        return fields[position];
    }

    public int length() {
        return fields.length;
    }

    @Override
    public Iterator<Field> iterator() {
        return Iterators.forArray(fields);
    }


    @Value
    public static class Field implements Serializable {

        private final String name;
        private final ScalarType type;
        private final boolean isNonNull;
        private final boolean isArray;
        private final boolean isPrimaryKey;

        public static Field simple(String name, ScalarType type) {
            return new Field(name, type,false, false, false);
        }

        public static Field primaryKey(String name, ScalarType type) {
            return new Field(name, type, true, false, true);
        }

        public static Field convert(String name, SourceTableSchema.Field field) {
            return new Field(name, field.getType(), field.isNotNull(), field.isArray(), false);
        }

    }

    public static class Builder {
        List<Field> fields;

        private Builder() {
            fields = new ArrayList<>();
        }

        public Builder add(Field field) {
            fields.add(field);
            return this;
        }

        public DestinationTableSchema build() {
            return new DestinationTableSchema(fields.toArray(new Field[fields.size()]));
        }


    }


}

