package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.NamePath;
import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.type.ScalarType;
import ai.dataeng.sqml.type.TypeMapping;
import com.google.common.base.Preconditions;
import lombok.*;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
public class SourceTableSchema implements Serializable {

    private final Table table;

    private SourceTableSchema(Table table) {
        this.table = table;
    }

    public void forEach(BiConsumer<String,Element> consumer) {
        table.forEach(consumer);
    }

    public SchemaAdjustment<SourceRecord> verifyAndAdjust(SourceRecord record, SchemaAdjustmentSettings settings) {
        SchemaAdjustment<Map<String,Object>> result = verifyAndAdjustTable(record.getData(), table, NamePath.ROOT, settings);
        return new SchemaAdjustment(result.transformedData()?record:null, result.getError());
    }

    private static SchemaAdjustment<Map<String,Object>> verifyAndAdjustTable(Map<String, Object> tableData, Table tableSchema, NamePath path, SchemaAdjustmentSettings settings) {
        Iterator<Map.Entry<String,Object>> dataIter = tableData.entrySet().iterator();
        int nonNullElements = 0;
        boolean transformed = false;
        boolean need2NormalizeNames = false;
        while (dataIter.hasNext()) {
            Map.Entry<String,Object> entry = dataIter.next();
            String name = entry.getKey();
            Object data = entry.getValue();
            String normalizedName = normalizeName(name);
            Element element = tableSchema.schema.get(normalizedName);
            if (element==null) {
                if (settings.dropFields()) {
                    dataIter.remove();
                } else {
                    return SchemaAdjustment.error(path.resolve(name),data,"Field is not defined in schema");
                }
            } else {
                if (!normalizedName.equals(name)) need2NormalizeNames = true;
                Object[] arr = null;
                if (data instanceof Collection || data.getClass().isArray()) {
                    if (data.getClass().isArray()) {
                        arr =(Object[])data;
                    } else {
                        Collection col = (Collection)data;
                        arr = new Object[col.size()];
                        int i=0;
                        for (Iterator it = col.iterator(); it.hasNext();) arr[i++]=it.next();
                        entry.setValue(arr);
                        transformed = true;
                    }
                    //Remove null values
                    int numNulls=0;
                    for (int i=0;i<arr.length;i++) {
                        if (arr[i]==null) numNulls++;
                    }
                    if (numNulls>0) {
                        if (settings.removeListNulls()) {
                            Object[] newArr = new Object[arr.length-numNulls];
                            int j=0;
                            for (int i=0;i<arr.length;i++) {
                                if (arr[i]!=null) newArr[j++]=arr[i];
                            }
                            entry.setValue(newArr);
                            arr = newArr;
                            transformed = true;
                        } else {
                            return SchemaAdjustment.error(path.resolve(name), arr, "Array contains null values");
                        }
                    }

                    if (!element.isArray()) {
                        if (arr.length<=1 && settings.array2Singleton()) {
                            if (arr.length==0) {
                                data = null;
                            } else {
                                data = arr[0];
                            }
                            entry.setValue(data);
                            transformed = true;
                        } else {
                            return SchemaAdjustment.error(path.resolve(name),arr,"Field is an array but expecting value");
                        }
                    }
                } else if (element.isArray()) {
                    if (data == null && settings.null2EmptyArray()) {
                        arr = new Object[0];
                        entry.setValue(arr);
                        transformed = true;
                    } else if (data != null && settings.singleton2Arrays()) {
                        arr = new Object[]{data};
                        entry.setValue(arr);
                        transformed = true;
                    } else {
                        return SchemaAdjustment.error(path.resolve(name),data,"Field is a value but expecting a list");
                    }
                }

                if (element.isNotNull()) nonNullElements++;
                if (data == null) {
                    if (element.isNotNull()) {
                        return SchemaAdjustment.error(path.resolve(name),data,"Field must be non-null");
                    }
                }

                if (element.isArray()) {
                    assert arr != null;
                    for (int i = 0; i < arr.length; i++) {
                        Object o = arr[i];
                        assert o!=null;
                        SchemaAdjustment<Object> result = verifyAndAdjustField(o,element,path.resolve(name),settings);
                        if (result.isError()) return result.castError();
                        else if (result.transformedData()) {
                            arr[i] = result.getData();
                            transformed = true;
                        }
                    }
                } else {
                    SchemaAdjustment<Object> result = verifyAndAdjustField(data,element,path.resolve(name),settings);
                    if (result.isError()) return result.castError();
                    else if (result.transformedData()) {
                        entry.setValue(result.getData());
                        transformed = true;
                    }
                }
            }
        }
        if (need2NormalizeNames) {
            List<String> nonNormalNames = tableData.keySet().stream().filter(name -> !name.equals(normalizeName(name))).collect(Collectors.toList());
            for (String name : nonNormalNames) {
                tableData.put(normalizeName(name),tableData.get(name));
                tableData.remove(name);
            }
        }
        if (nonNullElements < tableSchema.numNonNull()) {
            //Some non-null elements aren't in the record - we can now look them up directly since names have been normalized
            for (Map.Entry<String,Element> schemaElement : tableSchema.schema.entrySet()) {
                Element element = schemaElement.getValue();
                String elementName = schemaElement.getKey();
                if (element.isNotNull() && !tableData.containsKey(schemaElement.getKey())) {
                    if (element.isArray() && settings.null2EmptyArray()) {
                        tableData.put(elementName,Collections.EMPTY_LIST);
                        transformed = true;
                    } else {
                        return SchemaAdjustment.error(path.resolve(elementName),null,"Field must be non-null but missing in data");
                    }
                }
            }
        }
        if (transformed) return SchemaAdjustment.data(tableData);
        else return SchemaAdjustment.none();
    }

    private static SchemaAdjustment<Object> verifyAndAdjustField(@NonNull Object data, Element element, NamePath path, SchemaAdjustmentSettings settings) {
        if (element.isNestedTable()) {
            if (data instanceof Map) {
                SchemaAdjustment<Map<String,Object>> nestedAdjust = verifyAndAdjustTable((Map)data,(Table)element,path,settings);
                if (nestedAdjust.isError()) return nestedAdjust.castError();
                else if (nestedAdjust.transformedData()) return SchemaAdjustment.data(data);
                else return SchemaAdjustment.none();
            } else {
                return SchemaAdjustment.error(path,data,"Expected object");
            }
        } else {
            Field field = (Field) element;
            //Validate and/or cast data type
            return TypeMapping.adjustType(field.type, data, path, settings);
        }
    }

    public Element getElement(NamePath path) {
        return table.getElement(path);
    }


    @Getter @ToString @EqualsAndHashCode
    public static abstract class Element implements Serializable {

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

        public Field asField() {
            Preconditions.checkArgument(isField(),"Element is not a field: %s", this);
            return (Field)this;
        }

        public Table asTable() {
            Preconditions.checkArgument(isNestedTable(),"Element is not a table: %s", this);
            return (Table)this;
        }

    }

    @Getter @ToString @EqualsAndHashCode
    public static class Field extends Element {

        private final ScalarType type;

        private Field(boolean isArray, boolean notNull, ScalarType type) {
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

        private int numFields = -1;

        public int getNumFields() {
            if (numFields<0) {
                numFields = (int)schema.values().stream().filter(e -> e.isField()).count();
            }
            return numFields;
        }

        public Element getElement(NamePath path) {
            if (path.equals(NamePath.ROOT)) return this;
            Element base = this;
            for (String nested : path) {
                Preconditions.checkArgument(base instanceof Table, "Invalid path traverses through field: %s", path);
                Table t = (Table)base;
                base = t.schema.get(nested);
                Preconditions.checkArgument(base!=null, "Nested field [%s] of path [%s] does not exist", nested, path);
            }
            return base;
        }

        public Stream<Map.Entry<String,Element>> getFields() {
            return schema.entrySet().stream().filter(e -> e.getValue().isField());
        }

        private int numNonNull = -1;

        public int numNonNull() {
            if (numNonNull<0) {
                numNonNull = (int)schema.values().stream().filter(e -> e.isNotNull()).count();
            }
            return numNonNull;
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

        public Builder addField(String name, boolean isArray, boolean notNull, ScalarType type) {
            table.schema.put(normalizeName(name), new Field(isArray, notNull, type));
            return this;
        }

        public Builder addNestedTable(String name, boolean isArray, boolean notNull) {
            Table nestedtable = new Table(isArray, notNull);
            table.schema.put(normalizeName(name), nestedtable);
            return new Builder(nestedtable);
        }

        public SourceTableSchema build() {
            Preconditions.checkArgument(!table.schema.isEmpty(), "Empty schema");
            return new SourceTableSchema(table);
        }

    }

    public static String normalizeName(String name) {
        return name.trim().toLowerCase(Locale.ENGLISH);
    }

    public static Builder build() {
        return new Builder();
    }


}
