package ai.datasqrl.execute.flink.ingest.schema;

import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.FlexibleSchemaHelper;
import ai.datasqrl.schema.input.RelationType;
import ai.datasqrl.schema.constraint.ConstraintHelper;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class SourceRecord2RowMapper implements MapFunction<SourceRecord.Named, Row> {

    private final FlexibleDatasetSchema.TableField tableSchema;

    public SourceRecord2RowMapper(FlexibleDatasetSchema.TableField tableSchema) {
        this.tableSchema = tableSchema;
    }

    @Override
    public Row map(SourceRecord.Named sourceRecord) throws Exception {
        Object[] cols = constructRows(sourceRecord.getData(), tableSchema.getFields());
        //Add metadata
        cols = extendCols(cols,3);
        cols[0] = sourceRecord.getUuid().toString();
        cols[1] = sourceRecord.getIngestTime();
        cols[2] = sourceRecord.getSourceTime();
        return Row.ofKind(RowKind.INSERT, cols);
    }

    private static Object[] extendCols(Object[] cols, int paddingLength) {
        Object[] extendedCols = new Object[cols.length + paddingLength];
        System.arraycopy(cols,0,extendedCols,paddingLength, cols.length);
        return extendedCols;
    }

    private Object[] constructRows(Map<Name, Object> data,
                                   RelationType<FlexibleDatasetSchema.FlexibleField> schema) {
        return getFields(schema)
                .map(t -> {
                    Name name = t.getLeft();
                    FlexibleDatasetSchema.FieldType ftype = t.getMiddle();
                    if (ftype.getType() instanceof RelationType) {
                        RelationType<FlexibleDatasetSchema.FlexibleField> subType = (RelationType<FlexibleDatasetSchema.FlexibleField>) ftype.getType();
                        if (isSingleton(ftype)) {
                            return Row.of(constructRows((Map<Name, Object>) data.get(name), subType));
                        } else {
                            int idx = 0;
                            List<Map<Name, Object>> nestedData = (List<Map<Name, Object>>) data.get(name);
                            Row[] result = new Row[nestedData.size()];
                            for (Map<Name, Object> item : nestedData) {
                                Object[] cols = constructRows(item, subType);
                                //Add index
                                cols = extendCols(cols,1);
                                cols[0] = Long.valueOf(idx);
                                result[idx] = Row.of(cols);
                                idx++;
                            }
                            return result;
                        }
                    } else {
                        //Data is already correctly prepared by schema validation map-step
                        return data.get(name);
                    }
                })
                .toArray();
    }

    private static Stream<Triple<Name, FlexibleDatasetSchema.FieldType, Boolean>> getFields(
            RelationType<FlexibleDatasetSchema.FlexibleField> relation) {
        return relation.getFields().stream().flatMap(field -> field.getTypes().stream().map(ftype -> {
            Name name = FlexibleSchemaHelper.getCombinedName(field, ftype);
            boolean isMixedType = field.getTypes().size() > 1;
            return Triple.of(name, ftype, isMixedType);
        }));
    }

    private static boolean isSingleton(FlexibleDatasetSchema.FieldType ftype) {
        return ConstraintHelper.getCardinality(ftype.getConstraints()).isSingleton();
    }

}
