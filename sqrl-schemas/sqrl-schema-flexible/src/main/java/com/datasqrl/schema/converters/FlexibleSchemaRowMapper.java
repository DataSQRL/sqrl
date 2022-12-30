/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.name.Name;
import com.datasqrl.schema.constraint.ConstraintHelper;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.FlexibleDatasetSchema.FieldType;
import com.datasqrl.schema.input.FlexibleDatasetSchema.FlexibleField;
import com.datasqrl.schema.input.FlexibleDatasetSchema.TableField;
import com.datasqrl.schema.input.FlexibleSchemaHelper;
import com.datasqrl.schema.input.RelationType;
import java.util.ArrayList;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class FlexibleSchemaRowMapper<R> implements RowMapper<R>, Serializable {

  private final TableSchema schema;
  private final boolean hasSourceTimestamp;
  private final RowConstructor<R> rowConstructor;

  @Override
  public R apply(SourceRecord.Named sourceRecord) {
    Object[] cols = constructRows(sourceRecord.getData(), ((TableField) schema).getFields());
    //Add metadata
    cols = extendCols(cols, 2 + (hasSourceTimestamp ? 1 : 0));
    cols[0] = sourceRecord.getUuid().toString();
    cols[1] = sourceRecord.getIngestTime();
    if (hasSourceTimestamp) {
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
    List<Object> objects = new ArrayList<>();
    for (FlexibleField field : schema.getFields()) {
      for (FieldType type : field.getTypes()) {
        objects.add(constructRow(data, field, type));
      }
    }

    return objects.toArray();
  }

  private Object constructRow(Map<Name, Object> data, FlexibleField field, FieldType type) {
    Name name = FlexibleSchemaHelper.getCombinedName(field, type);
    boolean isMixedType = field.getTypes().size() > 1;
    if (type.getType() instanceof RelationType) {
      RelationType<FlexibleDatasetSchema.FlexibleField> subType =
          (RelationType<FlexibleDatasetSchema.FlexibleField>) type.getType();
      if (isSingleton(type)) {
        return rowConstructor.createNestedRow(
            constructRows((Map<Name, Object>) data.get(name), subType));
      } else {
        int idx = 0;
        List<Map<Name, Object>> nestedData = (List<Map<Name, Object>>) data.get(name);
        if (nestedData == null) {
          return null;
        }
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
  }

  private static boolean isSingleton(FlexibleDatasetSchema.FieldType ftype) {
    return ConstraintHelper.getCardinality(ftype.getConstraints()).isSingleton();
  }

}
