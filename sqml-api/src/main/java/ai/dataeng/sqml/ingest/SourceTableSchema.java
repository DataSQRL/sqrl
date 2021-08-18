package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.type.SqmlType;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

@Value
public class SourceTableSchema {

    private final Table table;

    private SourceTableSchema(Table table) {
        this.table = table;
    }

    public void forEach(BiConsumer<String,Element> consumer) {
        table.forEach(consumer);
    }

    public SourceRecord verifyAndAdjust(SourceRecord record, SchemaAdjustmentSettings settings) {
        return record;
    }

    @Getter @ToString @EqualsAndHashCode
    public static abstract class Element {

        private final boolean isArray;
        private final boolean notNull;

        private Element(boolean isArray, boolean notNull) {
            this.isArray = isArray;
            this.notNull = notNull;
        }

        public abstract boolean isField();

        public boolean isNestedTable() {
            return !isField();
        }

    }

    @Getter @ToString @EqualsAndHashCode
    public static class Field extends Element {

        private final SqmlType.ScalarSqmlType type;

        private Field(boolean isArray, boolean notNull, SqmlType.ScalarSqmlType type) {
            super(isArray, notNull);
            this.type = type;
        }

        @Override
        public boolean isField() {
            return true;
        }
    }

    @ToString @EqualsAndHashCode
    public static class Table extends Element {

        private final Map<String,Element> schema;

        private Table(boolean isArray, boolean notNull) {
            super(isArray, notNull);
            schema = new HashMap<>();
        }

        @Override
        public boolean isField() {
            return false;
        }

        public void forEach(BiConsumer<String,Element> consumer) {
            schema.forEach(consumer);
        }
    }



    public static class Builder {

        private Table table;

        private Builder(Table table) {
            this.table = table;
        }

        public Builder() {
            table = new Table(false, true);
        }

        public Builder addField(String name, boolean isArray, boolean notNull, SqmlType.ScalarSqmlType type) {
            table.schema.put(name, new Field(isArray, notNull, type));
            return this;
        }

        public Builder addNestedTable(String name, boolean isArray, boolean notNull) {
            Table nestedtable = new Table(isArray, notNull);
            table.schema.put(name, nestedtable);
            return new Builder(nestedtable);
        }

        public SourceTableSchema build() {
            Preconditions.checkArgument(!table.schema.isEmpty(),"Empty schema");
            return new SourceTableSchema(table);
        }

    }

    public static Builder build() {
        return new Builder();
    }


}
