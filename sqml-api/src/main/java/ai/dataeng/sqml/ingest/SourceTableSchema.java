package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.TypeMapping;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import lombok.*;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Value
public class SourceTableSchema {

    private final Table table;

    private SourceTableSchema(Table table) {
        this.table = table;
    }

    public void forEach(BiConsumer<String,Element> consumer) {
        table.forEach(consumer);
    }

    public SchemaAdjustment<SourceRecord> verifyAndAdjust(SourceRecord record, SchemaAdjustmentSettings settings) {
        SchemaAdjustment<Map<String,Object>> result = verifyAndAdjustTable(record.getData(), table, NamePath.BASE, settings);
        return new SchemaAdjustment(result.transformedData()?record:null, result.getError());
    }

    private static SchemaAdjustment<Map<String,Object>> verifyAndAdjustTable(Map<String, Object> tableData, Table tableSchema, NamePath path, SchemaAdjustmentSettings settings) {
        Iterator<Map.Entry<String,Object>> dataIter = tableData.entrySet().iterator();
        int nonNullElements = 0;
        boolean transformed = false;
        while (dataIter.hasNext()) {
            Map.Entry<String,Object> entry = dataIter.next();
            String name = entry.getKey();
            Object data = entry.getKey();
            Element element = tableSchema.schema.get(normalizeName(name));
            if (element==null) {
                if (settings.dropFields()) {
                    dataIter.remove();
                } else {
                    return SchemaAdjustment.error(path.sub(name),data,"Field is not defined in schema");
                }
            } else {
                List<Object> list = null;
                if (data instanceof List || data.getClass().isArray()) {
                    if (data.getClass().isArray()) {
                        list = Arrays.asList((Object[])data);
                        entry.setValue(list);
                        transformed = true;
                    } else list = (List)data;
                    if (!element.isArray()) {
                        if (list.size()<=1 && settings.array2Singleton()) {
                            if (list.isEmpty()) {
                                data = null;
                            } else {
                                data = Iterables.getOnlyElement(list);
                            }
                            entry.setValue(data);
                            transformed = true;
                        } else {
                            return SchemaAdjustment.error(path.sub(name),list,"Field is a list but expecting value");
                        }
                    }
                } else if (element.isArray()) {
                    if (data == null && settings.null2EmptyArray()) {
                        list = Collections.EMPTY_LIST;
                        entry.setValue(list);
                        transformed = true;
                    } else if (data != null && settings.singleton2Arrays()) {
                        list = Collections.singletonList(data);
                        entry.setValue(list);
                        transformed = true;
                    } else {
                        return SchemaAdjustment.error(path.sub(name),data,"Field is a value but expecting a list");
                    }
                }

                if (element.isNotNull()) nonNullElements++;
                if (data == null) {
                    if (element.isNotNull()) {
                        return SchemaAdjustment.error(path.sub(name),data,"Field must be non-null");
                    }
                }

                if (list!=null) {
                    for (int i = 0; i < list.size(); i++) {
                        Object o = list.get(i);
                        if (o==null) {
                            if (settings.removeListNulls()) {
                                list.remove(i);
                                i--;
                                transformed = true;
                            } else {
                                return SchemaAdjustment.error(path.sub(name),list,"List contains null values");
                            }
                        } else {
                            SchemaAdjustment<Object> result = verifyAndAdjustField(o,element,path.sub(name),settings);
                            if (result.isError()) return result.castError();
                            else if (result.transformedData()) {
                                list.set(i,result.getData());
                                transformed = true;
                            }
                        }
                    }
                } else {
                    SchemaAdjustment<Object> result = verifyAndAdjustField(data,element,path.sub(name),settings);
                    if (result.isError()) return result.castError();
                    else if (result.transformedData()) {
                        entry.setValue(result.getData());
                        transformed = true;
                    }
                }
            }
        }
        if (nonNullElements < tableSchema.numNonNull()) {
            //Some non-null elements aren't in the record
            Set<String> elements = tableSchema.schema.entrySet().stream().filter(e -> e.getValue().isNotNull()).map(e -> e.getKey()).collect(Collectors.toSet());
            tableData.keySet().forEach(name -> elements.remove(normalizeName(name)));
            for (String elementName : elements) {
                Element element = tableSchema.schema.get(elementName);
                if (element.isArray() && settings.null2EmptyArray()) {
                    tableData.put(elementName,Collections.EMPTY_LIST);
                    transformed = true;
                } else {
                    return SchemaAdjustment.error(path.sub(elementName),null,"Field must be non-null but missing in data");
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
            return TypeMapping.adjustType(field.type,data, path, settings);
        }
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

        public Builder addField(String name, boolean isArray, boolean notNull, SqmlType.ScalarSqmlType type) {
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
