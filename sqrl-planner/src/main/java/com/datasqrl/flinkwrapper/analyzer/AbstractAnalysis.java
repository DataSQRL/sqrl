package com.datasqrl.flinkwrapper.analyzer;

import com.datasqrl.util.CalciteUtil;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

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

  default String getFieldName(int idx) {
    return getField(idx).getName();
  }

  default int getFieldIndex(String fieldName) {
    RelDataTypeField field = getRowType().getField(fieldName, false, false);
    if (field == null) return -1;
    else return field.getIndex();
  }

  default Optional<Integer> getRowTime() {
    if (!hasRowType()) return Optional.empty();
    return CalciteUtil.findBestRowTimeIndex(getRowType());
  }

}
