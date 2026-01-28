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
package com.datasqrl.graphql.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * Bidirectional mapper between external Java objects and Flink's internal RowData representation.
 */
public class RowDataMapper {

  private final RowType inRowType;
  private final List<DataStructureConverter<Object, Object>> inFieldConverters;
  private final List<DataStructureConverter<Object, Object>> outFieldConverters;

  /**
   * Creates a new RowDataMapper for the specified row type schema.
   *
   * <p>Initializes field converters for each column in the row type, preparing them for
   * bidirectional conversion between external and internal representations.
   *
   * @param inRowType the Flink logical row type defining the schema and data types for conversion
   */
  public RowDataMapper(RowType inRowType, RowType outRowType) {
    this.inRowType = inRowType;
    inFieldConverters = createFieldConverters(inRowType);
    outFieldConverters = createFieldConverters(outRowType);
  }

  /**
   * Converts a map of field names to values into Flink's internal RowData representation.
   *
   * <p>This method transforms external Java objects (e.g., from GraphQL arguments) into Flink's
   * internal data format by applying appropriate type converters to each field value based on the
   * row type schema.
   *
   * @param args a map where keys are field names and values are the external representation of
   *     field values
   * @return a GenericRowData containing the converted internal representation of all fields
   */
  public GenericRowData toRowData(Map<String, Object> args) {
    var rowData = new GenericRowData(inRowType.getFieldCount());

    for (int i = 0; i < inRowType.getFieldCount(); i++) {
      var fieldName = inRowType.getFieldNames().get(i);
      var external = args.get(fieldName);
      var internal = inFieldConverters.get(i).toInternalOrNull(external);

      rowData.setField(i, internal);
    }

    return rowData;
  }

  /**
   * Converts Flink's internal RowData representation back to a list of external Java objects.
   *
   * <p>This method transforms Flink's internal data format back into external Java objects by
   * applying appropriate type converters to each field. The resulting list maintains the same field
   * order as defined in the row type schema.
   *
   * @param rowData the internal Flink row data to convert
   * @return a list of external values in the same order as the row type field definitions
   */
  public List<Object> fromRowData(GenericRowData rowData) {
    var res = new ArrayList<>(rowData.getArity());

    for (int i = 0; i < rowData.getArity(); i++) {
      var internal = rowData.getField(i);
      var external = outFieldConverters.get(i).toExternalOrNull(internal);

      res.add(external);
    }

    return res;
  }

  private List<DataStructureConverter<Object, Object>> createFieldConverters(RowType rowType) {
    var rowDataType = TypeConversions.fromLogicalToDataType(rowType);
    var converters = new ArrayList<DataStructureConverter<Object, Object>>(rowType.getFieldCount());

    for (int i = 0; i < rowType.getFieldCount(); i++) {
      var fieldDataType = rowDataType.getChildren().get(i);
      var converter = DataStructureConverters.getConverter(fieldDataType);
      converter.open(getClass().getClassLoader());

      converters.add(converter);
    }

    return converters;
  }
}
