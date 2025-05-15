package com.datasqrl.planner.analyzer;

import java.util.Optional;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.datasqrl.util.CalciteUtil;

public interface AbstractAnalysis {

  RelNode getRelNode();

  default boolean hasRowType() {
    return getRelNode() != null;
  }

  default RelDataType getRowType() {
    return getRelNode().getRowType();
  }

  default int getFieldLength() {
    return getRowType().getFieldCount();
  }

  default RelDataTypeField getField(int idx) {
    return getRowType().getFieldList().get(idx);
  }

  default RelDataTypeField getField(String name) {
    return getRowType().getField(name, false, false);
  }

  default String getFieldName(int idx) {
    return getField(idx).getName();
  }

  default int getFieldIndex(String fieldName) {
    RelDataTypeField field = getRowType().getField(fieldName, false, false);
    if (field == null) {
        return -1;
    } else {
        return field.getIndex();
    }
  }

  default boolean hasField(String fieldName) {
    return getRowType().getField(fieldName, false, false)!=null;
  }

  default Optional<Integer> getRowTime() {
    if (!hasRowType()) {
        return Optional.empty();
    }
    return CalciteUtil.findBestRowTimeIndex(getRowType());
  }

}
