/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.planner.analyzer;

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

  /**
   * Looks up a column by its exact name, returns null if there is no such column. A column name
   * that a user writes in a hint or in {@code compiler.tags} resolves through {@link
   * ColumnReference} instead.
   */
  default RelDataTypeField getField(String name) {
    return getRowType().getField(name, true, false);
  }

  default String getFieldName(int idx) {
    return getField(idx).getName();
  }

  /** The index of the column with the exact name, or -1 if there is no such column. */
  default int getFieldIndex(String fieldName) {
    var field = getField(fieldName);

    return field != null ? field.getIndex() : -1;
  }

  default Optional<Integer> getRowTime() {
    if (!hasRowType()) {
      return Optional.empty();
    }
    return CalciteUtil.findBestRowTimeIndex(getRowType());
  }
}
