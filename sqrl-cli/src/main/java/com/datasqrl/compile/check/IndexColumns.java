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
package com.datasqrl.compile.check;

import com.datasqrl.compile.check.SymbolIndex.Column;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.List;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

/** The columns of the index, from the row types of the planner. */
@UtilityClass
class IndexColumns {

  static List<Column> columns(RelDataType rowType, Documentation documentation) {
    return rowType.getFieldList().stream()
        .map(
            field ->
                new Column(
                    field.getName(),
                    typeName(field.getType()),
                    documentation.getColumnOpt(field.getName()),
                    fields(field.getType())))
        .toList();
  }

  /** The fields of a row type, e.g. of a column that holds a nested JSON object. */
  static List<Column> fields(RelDataType type) {
    if (!type.isStruct()) {
      return List.of();
    }
    return type.getFieldList().stream()
        .map(
            field ->
                new Column(
                    field.getName(),
                    typeName(field.getType()),
                    Optional.empty(),
                    fields(field.getType())))
        .toList();
  }

  static String typeName(RelDataType type) {
    try {
      return FlinkTypeFactory.toLogicalType(type).asSummaryString();
    } catch (RuntimeException e) {
      return type.getFullTypeString();
    }
  }
}
