package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.type.DateTimeType;
import ai.dataeng.sqml.type.IntegerType;
import ai.dataeng.sqml.type.ScalarType;
import ai.dataeng.sqml.type.UuidType;
import lombok.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Value
public class RecordShredder {

    private final DestinationTableSchema resultSchema;
    private final RecordShredderFlatMap process;


    public static RecordShredder from(NamePath tableIdentifier, FieldProjection[][] keyProjections, SourceTableSchema schema, TimestampSelector timestampSelector) {
        Preconditions.checkArgument(keyProjections.length==tableIdentifier.getNumComponents()+1,"Invalid number of projections provided: %s", keyProjections);

        DestinationTableSchema.Builder builder = DestinationTableSchema.builder();
        List<String> remainingTableFields = new ArrayList<>();
        //Add parent keys
        for (int depth = 0; depth < keyProjections.length; depth ++) {
            FieldProjection[] proj = keyProjections[depth];
            NamePath base = tableIdentifier.prefix(depth);

            for (int projNo = 0; projNo < proj.length; projNo++) {
                String prefix = "";
                if (depth < keyProjections.length-1) prefix = "_parent"+depth+"_"+projNo;

                builder.add(proj[projNo].getField(schema.getElement(base).asTable()));
            }
        }
        //Add table columns
        SourceTableSchema.Table table = schema.getElement(tableIdentifier).asTable();
        table.getFields().forEach(e -> {
            //only add field if it is not already part of the key, i.e. none of the innermost FieldProjection reference that field
            String fieldName = e.getKey();
            boolean isKey = false;
            for (FieldProjection proj : keyProjections[keyProjections.length-1]) {
                if (proj instanceof NamePathProjection) {
                    NamePath np = ((NamePathProjection)proj).path;
                    if (np.getNumComponents()==1 && np.getComponent(0).equals(fieldName)) isKey = true;
                }
            }
            if (!isKey) {
                builder.add(DestinationTableSchema.Field.convert(fieldName, e.getValue().asField()));
                remainingTableFields.add(fieldName);
            }
        });

        //Add Timestamps from SourceRecord
        builder.add(DestinationTableSchema.Field.simple("__timestamp", DateTimeType.INSTANCE));
        DestinationTableSchema resultSchema = builder.build();

        return new RecordShredder(resultSchema,
                new RecordShredderFlatMap(tableIdentifier,resultSchema.length(),
                        remainingTableFields.toArray(new String[remainingTableFields.size()]),
                        keyProjections, timestampSelector));
    }

    public static RecordShredder from(NamePath tableIdentifier, SourceTableSchema schema) {
        return from(tableIdentifier,defaultParentKeys(tableIdentifier,schema),schema, TimestampSelector.SOURCE_TIME);
    }

    private static final FieldProjection[][] defaultParentKeys(NamePath tableIdentifier, SourceTableSchema schema) {
        int depth = tableIdentifier.getNumComponents();
        FieldProjection[][] projs = new FieldProjection[depth+1][];
        for (int i = 0; i < projs.length; i++) {
            if (i==0) {
                projs[i]=new FieldProjection[]{ROOT_UUID};
            } else if (schema.getTable().getElement(tableIdentifier.prefix(i)).asTable().isArray()) {
                projs[i]=new FieldProjection[]{ARRAY_INDEX};
            } else {
                projs[i]=new FieldProjection[0];
            }
        }
        return projs;
    }

    public interface FieldProjection extends Serializable {

        DestinationTableSchema.Field getField(SourceTableSchema.Table table);

        Object getData(Map<String,Object> data);

    }

    @Value
    public static class SpecialFieldProjection implements FieldProjection {

        private final String name;
        private final ScalarType type;

        @Override
        public DestinationTableSchema.Field getField(SourceTableSchema.Table table) {
            return DestinationTableSchema.Field.primaryKey(name, type);
        }

        @Override
        public Object getData(Map<String, Object> data) {
            throw new UnsupportedOperationException();
        }
    }

    public static final FieldProjection ROOT_UUID = new SpecialFieldProjection("_uuid", UuidType.INSTANCE);

    public static final FieldProjection ARRAY_INDEX = new SpecialFieldProjection("_idx", IntegerType.INSTANCE);

    public static class NamePathProjection implements FieldProjection {

        private final NamePath path;

        public NamePathProjection(NamePath path) {
            Preconditions.checkArgument(path.getNumComponents()>0);
            this.path = path;
        }

        @Override
        public DestinationTableSchema.Field getField(SourceTableSchema.Table table) {
            SourceTableSchema.Field sourceField = table.getElement(path).asField();
            Preconditions.checkArgument(!sourceField.isArray(),"A primary key projection cannot be of type ARRAY");
            return DestinationTableSchema.Field.primaryKey(path.toString('_'),sourceField.getType());
        }

        @Override
        public Object getData(Map<String, Object> data) {
            Map<String, Object> base = data;
            for (int i = 0; i < path.getNumComponents()-2; i++) {
                Object map = base.get(path.getComponent(i));
                Preconditions.checkArgument(map instanceof Map, "Illegal field projection");
                base = (Map)map;
            }
            return base.get(path.getLastComponent());
        }
    }

    public static class RecordShredderFlatMap implements FlatMapFunction<SourceRecord, Row> {

        private final NamePath tableIdentifier;
        private final int rowLength;
        private final String[] sourceTableFields;
        private final FieldProjection[][] keyProjections;
        private final TimestampSelector timestampSelector;

        public RecordShredderFlatMap(NamePath tableIdentifier, int rowLength, String[] sourceTableFields,
                                     FieldProjection[][] keyProjections, TimestampSelector timestampSelector) {
            this.tableIdentifier = tableIdentifier;
            this.rowLength = rowLength;
            this.sourceTableFields = sourceTableFields;
            this.keyProjections = keyProjections;
            this.timestampSelector = timestampSelector;
        }

        @Override
        public void flatMap(SourceRecord sourceRecord, Collector<Row> collector) throws Exception {
            Object[] cols = new Object[rowLength];
            int[] internalIdx = new int[tableIdentifier.getNumComponents()];
            int colno = 0;
            if (keyProjections[0].length > 0 && keyProjections[0][0].equals(ROOT_UUID)) {
                //Special case of using uuid for global key
                Preconditions.checkArgument(sourceRecord.hasUUID());
                cols[colno++] = sourceRecord.getUuid().toString();
            }
            //Add timestamps
            cols[cols.length - 1] = timestampSelector.getTimestamp(sourceRecord);
            //Construct rows iteratively
            constructRows(sourceRecord.getData(), cols, colno, 0, collector);
        }

        private void constructRows(Map<String, Object> data, Object[] cols, int colno, int depth,
                                   Collector<Row> collector) {
            //Add projections at the current level
            for (FieldProjection proj : keyProjections[depth]) {
                //We can ignore special projections here since those are handled at the outer level
                if (proj instanceof SpecialFieldProjection) continue;
                Preconditions.checkArgument(proj instanceof NamePathProjection);
                cols[colno++] = proj.getData(data);
            }

            if (depth == tableIdentifier.getNumComponents()) {
                //We have reached the most inner table and maximum depth - collect table data and wrap up row
                for (int i = 0; i < sourceTableFields.length; i++) {
                    cols[colno++] = data.get(sourceTableFields[i]);
                }
                Row row = Row.ofKind(RowKind.INSERT, cols);
                //System.out.println("Shredded row: " + row);
                collector.collect(row);
            } else {
                //We have to get into nested table
                Object nested = data.get(tableIdentifier.getComponent(depth));
                if (nested instanceof Map) {
                    constructRows((Map) nested, cols, colno, depth + 1, collector);
                } else if (nested.getClass().isArray()) {
                    Object[] arr = (Object[]) nested;
                    boolean addIndex = keyProjections[depth + 1].length > 0 && keyProjections[depth + 1][0].equals(ARRAY_INDEX);
                    for (int i = 0; i < arr.length; i++) {
                        Object[] colcopy = cols.clone();
                        if (addIndex) {
                            colcopy[colno] = Long.valueOf(i);
                        }
                        constructRows((Map) arr[i], colcopy, colno + (addIndex ? 1 : 0), depth + 1, collector);
                    }
                } else throw new IllegalArgumentException("Unexpected data encountered: " + nested);
            }
        }

    }


}
