/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.io.SourceRecord.Named;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.FieldList;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.UniversalTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class UniversalTableRowMapper<R> implements RowMapper<R> {

  private final UniversalTable universalTable;
  private final RowConstructor<R> rowConstructor;

  @Override
  public R apply(Named sourceRecord) {
    Object[] cols = constructRows(sourceRecord.getData(), universalTable.getFields());
    //Add metadata
    cols = extendCols(cols, 2 + (universalTable.isHasSourceTimestamp() ? 1 : 0));
    cols[0] = sourceRecord.getUuid().toString();
    cols[1] = sourceRecord.getIngestTime();
    if (universalTable.isHasSourceTimestamp()) {
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
      FieldList fieldList) {
    List<Object> objects = new ArrayList<>();
    for (Field field : fieldList.getFields()) {
      objects.add(constructRow(data, field));
    }

    return objects.toArray();
  }

  private Object constructRow(Map<Name, Object> data, Field field) {
    Name name = field.getName();
    if (field instanceof UniversalTable.ChildRelationship) {
      UniversalTable.ChildRelationship rel = (UniversalTable.ChildRelationship)field;
      if (isSingleton(rel.getMultiplicity())) {
        return rowConstructor.createNestedRow(
            constructRows((Map<Name, Object>) data.get(name), rel.getChildTable().getFields()));
      } else {
        int idx = 0;
        List<Map<Name, Object>> nestedData = (List<Map<Name, Object>>) data.get(name);
        Object[] rows = new Object[nestedData.size()];
        for (Map<Name, Object> item : nestedData) {
          Object[] cols = constructRows(item, rel.getChildTable().getFields());
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

  private static boolean isSingleton(Multiplicity multiplicity) {
    return multiplicity != Multiplicity.MANY;
  }

}
