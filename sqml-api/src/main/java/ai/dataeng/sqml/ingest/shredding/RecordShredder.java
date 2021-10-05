package ai.dataeng.sqml.ingest.shredding;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.ingest.NamePathOld;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SourceTableSchema;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.name.NamePath;
import lombok.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Value
public class RecordShredder {

    private final DestinationTableSchema resultSchema;
    private final RecordShredderFlatMap process;


    public static RecordShredder from(NamePath tableIdentifier, FieldProjection[][] keyProjections,
                                      RelationType<StandardField> tableSchema, RecordProjection[] recordProjections) {
        Preconditions.checkArgument(keyProjections.length==tableIdentifier.getLength()+1,
                "Invalid number of projections provided: %s", keyProjections);

        DestinationTableSchema.Builder builder = DestinationTableSchema.builder();
        List<String> remainingTableFields = new ArrayList<>();
        //Add parent keys
        for (int depth = 0; depth < keyProjections.length; depth ++) {
            FieldProjection[] proj = keyProjections[depth];
            NamePath base = tableIdentifier.prefix(depth);

            for (int projNo = 0; projNo < proj.length; projNo++) {
                String prefix = "";
                if (depth < keyProjections.length-1) prefix = "_parent"+depth+"_"+projNo;

                builder.add(proj[projNo].getField(tableSchema.getElement(base).asTable()));
            }
        }
        //Add table columns
        SourceTableSchema.Table table = tableSchema.getElement(tableIdentifier).asTable();
        table.getFields().forEach(e -> {
            //only add field if it is not already part of the key, i.e. none of the innermost FieldProjection reference that field
            String fieldName = e.getKey();
            boolean isKey = false;
            for (FieldProjection proj : keyProjections[keyProjections.length-1]) {
                if (proj instanceof FieldProjection.NamePath) {
                    NamePathOld np = ((FieldProjection.NamePath)proj).getPath();
                    if (np.getNumComponents()==1 && np.getComponent(0).equals(fieldName)) isKey = true;
                }
            }
            if (!isKey) {
                builder.add(DestinationTableSchema.Field.convert(fieldName, e.getValue().asField()));
                remainingTableFields.add(fieldName);
            }
        });

        //Add RecordProjections at the end
        for (int i = 0; i < recordProjections.length; i++) {
            builder.add(recordProjections[i].getField());
        }
        DestinationTableSchema resultSchema = builder.build();

        return new RecordShredder(resultSchema,
                new RecordShredderFlatMap(tableIdentifier,resultSchema.length(),
                        remainingTableFields.toArray(new String[remainingTableFields.size()]),
                        keyProjections, recordProjections));
    }

    public static RecordShredder from(NamePathOld tableIdentifier, SourceTableSchema schema) {
        return from(tableIdentifier,defaultParentKeys(tableIdentifier,schema),schema, new RecordProjection[]{RecordProjection.DEFAULT_TIMESTAMP});
    }

    private static final FieldProjection[][] defaultParentKeys(NamePathOld tableIdentifier, SourceTableSchema schema) {
        int depth = tableIdentifier.getNumComponents();
        FieldProjection[][] projs = new FieldProjection[depth+1][];
        for (int i = 0; i < projs.length; i++) {
            if (i==0) {
                projs[i]=new FieldProjection[]{FieldProjection.ROOT_UUID};
            } else if (schema.getTable().getElement(tableIdentifier.prefix(i)).asTable().isArray()) {
                projs[i]=new FieldProjection[]{FieldProjection.ARRAY_INDEX};
            } else {
                projs[i]=new FieldProjection[0];
            }
        }
        return projs;
    }





    public static class RecordShredderFlatMap implements FlatMapFunction<SourceRecord, Row> {

        private final NamePathOld tableIdentifier;
        private final int rowLength;
        private final String[] sourceTableFields;
        private final FieldProjection[][] keyProjections;
        private final RecordProjection[] recordProjections;

        public RecordShredderFlatMap(NamePathOld tableIdentifier, int rowLength, String[] sourceTableFields,
                                     FieldProjection[][] keyProjections, RecordProjection[] recordProjections) {
            this.tableIdentifier = tableIdentifier;
            this.rowLength = rowLength;
            this.sourceTableFields = sourceTableFields;
            this.keyProjections = keyProjections;
            this.recordProjections = recordProjections;
        }

        @Override
        public void flatMap(SourceRecord sourceRecord, Collector<Row> collector) throws Exception {
            Object[] cols = new Object[rowLength];
            int[] internalIdx = new int[tableIdentifier.getNumComponents()];
            int colno = 0;
            if (keyProjections[0].length > 0 && keyProjections[0][0].equals(FieldProjection.ROOT_UUID)) {
                //Special case of using uuid for global key
                Preconditions.checkArgument(sourceRecord.hasUUID());
                cols[colno++] = sourceRecord.getUuid().toString();
            }
            //Add Record Projections at the end
            for (int i = 0; i < recordProjections.length; i++) {
                int offset = cols.length-recordProjections.length+i;
                cols[offset] = recordProjections[i].getData(sourceRecord);
            }
            //Construct rows iteratively
            constructRows(sourceRecord.getData(), cols, colno, 0, collector);
        }

        private void constructRows(Map<String, Object> data, Object[] cols, int colno, int depth,
                                   Collector<Row> collector) {
            //Add projections at the current level
            for (FieldProjection proj : keyProjections[depth]) {
                //We can ignore special projections here since those are handled at the outer level
                if (proj instanceof FieldProjection.SpecialCase) continue;
                Preconditions.checkArgument(proj instanceof FieldProjection.NamePath);
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
                    boolean addIndex = keyProjections[depth + 1].length > 0 && keyProjections[depth + 1][0].equals(FieldProjection.ARRAY_INDEX);
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
