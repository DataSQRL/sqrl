package ai.dataeng.sqml.ingest.shredding;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.schema2.*;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NamePath;
import static ai.dataeng.sqml.schema2.TypeHelper.*;
import lombok.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Value
public class RecordShredder {

    private final DestinationTableSchema resultSchema;
    private final RecordShredderFlatMap process;
    private final NamePath tableIdentifier;


    public static RecordShredder from(final NamePath tableIdentifier, FieldProjection[][] keyProjections,
                                      RelationType<StandardField> schema, RecordProjection[] recordProjections) {
        Preconditions.checkArgument(keyProjections.length==tableIdentifier.getLength()+1,
                "Invalid number of projections provided: %s", Arrays.deepToString(keyProjections));

        DestinationTableSchema.Builder builder = DestinationTableSchema.builder();
        List<Name> remainingTableFields = new ArrayList<>();
        //Add parent keys
        for (int depth = 0; depth < keyProjections.length; depth ++) {
            FieldProjection[] proj = keyProjections[depth];
            NamePath base = tableIdentifier.prefix(depth);

            for (int projNo = 0; projNo < proj.length; projNo++) {
                String prefix = "";
                if (depth < keyProjections.length-1) prefix = "_parent"+depth+"_"+projNo;

                builder.add(proj[projNo].getField(getNestedRelation(schema,base)));
            }
        }
        //Add table columns
        RelationType<StandardField> tableSchema = getNestedRelation(schema,tableIdentifier);
        tableSchema.getFields().forEach(field -> {
            //only add field if it is not already part of the key, i.e. none of the innermost FieldProjection reference that field
            Name fieldName = field.getName();
            boolean isKey = false;
            for (FieldProjection proj : keyProjections[keyProjections.length-1]) {
                if (proj instanceof FieldProjection.NamePathProjection) {
                    NamePath np = ((FieldProjection.NamePathProjection)proj).getPath();
                    if (np.getLength()==1 && np.get(0).equals(fieldName)) isKey = true;
                }
            }
            if (!isKey && (TypeHelper.unfurlType(field.getType()) instanceof BasicType)) { //filter out keys and nested tables
                builder.add(DestinationTableSchema.Field.convert(fieldName.getCanonical(), field));
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
                        remainingTableFields.toArray(new Name[remainingTableFields.size()]),
                        keyProjections, recordProjections),
                tableIdentifier);
    }

    public static RecordShredder from(NamePath tableIdentifier, RelationType<StandardField> schema) {
        return from(tableIdentifier, defaultParentKeys(tableIdentifier, schema), schema, new RecordProjection[]{RecordProjection.DEFAULT_TIMESTAMP});
    }

    private static FieldProjection[][] defaultParentKeys(NamePath tableIdentifier, RelationType<StandardField> schema) {
        int depth = tableIdentifier.getLength();
        FieldProjection[][] projs = new FieldProjection[depth+1][];
        for (int i = 0; i < projs.length; i++) {
            if (i==0) {
                projs[i]=new FieldProjection[]{FieldProjection.ROOT_UUID};
            } else if (getNestedType(schema,tableIdentifier.prefix(i)) instanceof ArrayType) {
                projs[i]=new FieldProjection[]{FieldProjection.ARRAY_INDEX};
            } else {
                projs[i]=new FieldProjection[0];
            }
        }
        return projs;
    }

    public static List<RecordShredder> from(final RelationType<StandardField> tableSchema) {
        List<RecordShredder> shredders = new ArrayList<>();
        findNestedTables(NamePath.ROOT, tableSchema, tableSchema, shredders);
        return shredders;
    }

    private static void findNestedTables(NamePath tableIdentifier, RelationType<StandardField> nestedTableSchema,
                                         RelationType<StandardField> rootTableSchema, List<RecordShredder> shredders) {
        shredders.add(from(tableIdentifier,rootTableSchema));
        for (StandardField field : nestedTableSchema) {
            Type type = TypeHelper.unfurlType(field.getType());
            if (type instanceof RelationType) {
                findNestedTables(tableIdentifier.resolve(field.getName()), (RelationType)type, rootTableSchema, shredders);
            }
        }
    }



    public static class RecordShredderFlatMap implements FlatMapFunction<SourceRecord<Name>, Row> {

        private final NamePath tableIdentifier;
        private final int rowLength;
        private final Name[] sourceTableFields;
        private final FieldProjection[][] keyProjections;
        private final RecordProjection[] recordProjections;

        public RecordShredderFlatMap(NamePath tableIdentifier, int rowLength, Name[] sourceTableFields,
                                     FieldProjection[][] keyProjections, RecordProjection[] recordProjections) {
            this.tableIdentifier = tableIdentifier;
            this.rowLength = rowLength;
            this.sourceTableFields = sourceTableFields;
            this.keyProjections = keyProjections;
            this.recordProjections = recordProjections;
        }

        @Override
        public void flatMap(SourceRecord<Name> sourceRecord, Collector<Row> collector) throws Exception {
            Object[] cols = new Object[rowLength];
            int[] internalIdx = new int[tableIdentifier.getLength()];
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

        private void constructRows(Map<Name, Object> data, Object[] cols, int colno, int depth,
                                   Collector<Row> collector) {
            //Add projections at the current level
            for (FieldProjection proj : keyProjections[depth]) {
                //We can ignore special projections here since those are handled at the outer level
                if (proj instanceof FieldProjection.SpecialCase) continue;
                Preconditions.checkArgument(proj instanceof FieldProjection.NamePathProjection);
                cols[colno++] = proj.getData(data);
            }

            if (depth == tableIdentifier.getLength()) {
                //We have reached the most inner table and maximum depth - collect table data and wrap up row
                for (int i = 0; i < sourceTableFields.length; i++) {
                    cols[colno++] = data.get(sourceTableFields[i]);
                }
                Row row = Row.ofKind(RowKind.INSERT, cols);
                //System.out.println("Shredded row: " + row);
                collector.collect(row);
            } else {
                //We have to get into nested table
                Object nested = data.get(tableIdentifier.get(depth));
                if (nested instanceof Map) {
                    constructRows((Map) nested, cols, colno, depth + 1, collector);
                } else if (nested instanceof List) {
                    List arr = (List)nested;
                    boolean addIndex = keyProjections[depth + 1].length > 0 && keyProjections[depth + 1][0].equals(FieldProjection.ARRAY_INDEX);
                    for (int i = 0; i < arr.size(); i++) {
                        Object[] colcopy = cols.clone();
                        if (addIndex) {
                            colcopy[colno] = Long.valueOf(i);
                        }
                        constructRows((Map) arr.get(i), colcopy, colno + (addIndex ? 1 : 0), depth + 1, collector);
                    }
                } else throw new IllegalArgumentException("Unexpected data encountered: " + nested);
            }
        }

    }


}
