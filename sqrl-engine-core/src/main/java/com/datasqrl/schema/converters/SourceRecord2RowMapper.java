/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.io.SourceRecord;
import com.datasqrl.name.Name;
import com.datasqrl.schema.constraint.ConstraintHelper;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.FlexibleSchemaHelper;
import com.datasqrl.schema.input.InputTableSchema;
import com.datasqrl.schema.input.RelationType;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Triple;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

@AllArgsConstructor
public class SourceRecord2RowMapper<R> implements Function<SourceRecord.Named, R>, Serializable {

  private final InputTableSchema tableSchema;
  private final RowConstructor<R> rowConstructor;

  public R apply(SourceRecord.Named sourceRecord) {
    Object[] cols = constructRows(sourceRecord.getData(), tableSchema.getSchema().getFields());
    //Add metadata
    cols = extendCols(cols, 2 + (tableSchema.isHasSourceTimestamp() ? 1 : 0));
    cols[0] = sourceRecord.getUuid().toString();
    cols[1] = sourceRecord.getIngestTime();
    if (tableSchema.isHasSourceTimestamp()) {
      cols[2] = sourceRecord.getSourceTime();
    }
    return rowConstructor.createRow(cols);
  }

  private static Object[] extendCols(Object[] cols, int paddingLength) {
    Object[] extendedCols = new Object[cols.length + paddingLength];
    System.arraycopy(cols, 0, extendedCols, paddingLength, cols.length);
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
              return rowConstructor.createNestedRow(
                  constructRows((Map<Name, Object>) data.get(name), subType));
            } else {
              int idx = 0;
              List<Map<Name, Object>> nestedData = (List<Map<Name, Object>>) data.get(name);
              Object[] rows = new Object[nestedData.size()];
              for (Map<Name, Object> item : nestedData) {
                Object[] cols = constructRows(item, subType);
                //Add index
                cols = extendCols(cols, 1);
                cols[0] = Integer.valueOf(idx);
                rows[idx] = rowConstructor.createNestedRow(cols);
                idx++;
              }
              return rowConstructor.createRowList(rows);
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

  public interface RowConstructor<R> extends Serializable {

    R createRow(Object[] columns);

    Object createNestedRow(Object[] columns);

    Object createRowList(Object[] rows);

  }

}
