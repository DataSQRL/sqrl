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

import com.datasqrl.graphql.server.FunctionExecutor;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

@RequiredArgsConstructor
public class FlinkFunctionExecutor implements FunctionExecutor {

  private final Optional<FlinkExecFunctionPlan> optPlan;

  @Override
  public Object execute(DataFetchingEnvironment env, String functionId) {

    if (optPlan.isEmpty()) {
      throw new IllegalStateException("Exec function plan is empty");
    }

    var plan = optPlan.get();
    var fn =
        plan.getFunction(functionId)
            .orElseThrow(
                () -> new IllegalArgumentException("Function " + functionId + " not found"));

    var inputType = fn.getInputType();
    validateInputFields(env, inputType);

    fn.instantiateFunction(getClass().getClassLoader());

    var converter = new RowTypeConverter(inputType);
    var rowData = converter.toInternal(env.getArguments());

    var internalRes = fn.execute(List.of(rowData));
    var res = converter.toExternal((GenericRowData) internalRes.get(0));

    return res.get(0);
  }

  private void validateInputFields(DataFetchingEnvironment env, RowType inputType) {
    var missingFields =
        inputType.getFieldNames().stream()
            .filter(fieldName -> !env.getArguments().containsKey(fieldName))
            .collect(Collectors.toList());

    if (!missingFields.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot execute function. Missing required input fields: "
              + String.join(", ", missingFields));
    }
  }

  public static class RowTypeConverter {

    private final RowType rowType;
    private final List<DataStructureConverter<Object, Object>> fieldConverters;

    public RowTypeConverter(RowType rowType) {
      this.rowType = rowType;
      var rowDataType = TypeConversions.fromLogicalToDataType(rowType);

      this.fieldConverters = new ArrayList<>(rowType.getFieldCount());
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        var fieldDataType = rowDataType.getChildren().get(i);
        var converter = DataStructureConverters.getConverter(fieldDataType);
        converter.open(getClass().getClassLoader());

        fieldConverters.add(converter);
      }
    }

    public GenericRowData toInternal(Map<String, Object> args) {
      var rowData = new GenericRowData(rowType.getFieldCount());

      for (int i = 0; i < rowType.getFieldCount(); i++) {
        var fieldName = rowType.getFieldNames().get(i);
        var external = args.get(fieldName);
        var internal = fieldConverters.get(i).toInternalOrNull(external);

        rowData.setField(i, internal);
      }

      return rowData;
    }

    public List<Object> toExternal(GenericRowData rowData) {
      var res = new ArrayList<>(rowData.getArity());

      for (int i = 0; i < rowData.getArity(); i++) {
        var internal = rowData.getField(i);
        var external = fieldConverters.get(i).toExternalOrNull(internal);

        res.add(external);
      }

      return res;
    }
  }
}
