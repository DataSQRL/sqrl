package ai.datasqrl.schema.converters;

import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.constraint.ConstraintHelper;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.FlexibleSchemaHelper;
import ai.datasqrl.schema.input.RelationType;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Triple;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

@AllArgsConstructor
public class SourceRecord2RowMapper<R,S> implements Function<SourceRecord.Named, R>, Serializable {

    private final FlexibleDatasetSchema.TableField tableSchema;
    private final RowConstructor<R,S> rowConstructor;

    public R apply(SourceRecord.Named sourceRecord) {
        Object[] cols = constructRows(sourceRecord.getData(), tableSchema.getFields());
        //Add metadata
        cols = extendCols(cols,3);
        cols[0] = sourceRecord.getUuid().toString();
        cols[1] = sourceRecord.getIngestTime();
        cols[2] = sourceRecord.getSourceTime();
        return rowConstructor.createRoot(cols);
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
                            return rowConstructor.createNested(constructRows((Map<Name, Object>) data.get(name), subType));
                        } else {
                            int idx = 0;
                            List<Map<Name, Object>> nestedData = (List<Map<Name, Object>>) data.get(name);
                            S[] result = rowConstructor.createRowArray(nestedData.size());
                            for (Map<Name, Object> item : nestedData) {
                                Object[] cols = constructRows(item, subType);
                                //Add index
                                cols = extendCols(cols,1);
                                cols[0] = Long.valueOf(idx);
                                result[idx] = rowConstructor.createNested(cols);
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

    public interface RowConstructor<R,S> extends Serializable {

        R createRoot(Object[] columns);

        S createNested(Object[] columns);

        S[] createRowArray(int size);

    }

}
