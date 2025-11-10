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

import java.util.List;
import java.util.Optional;
import lombok.NoArgsConstructor;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator$;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.RowType;
import scala.Option;
import scala.Some;

@NoArgsConstructor
final class CodeGenBridge {

  public static GeneratedFunction<FlatMapFunction<RowData, RowData>> gen(
      RowType inType,
      String name,
      RowType outType,
      Class<? extends RowData> outRowClz,
      List<RexNode> projection,
      Optional<RexNode> condition,
      ReadableConfig cfg,
      ClassLoader cl) {

    // delegate to the Scala object
    return CalcCodeGenerator$.MODULE$.generateFunction(
        inType,
        name,
        outType,
        outRowClz,
        scala.collection.JavaConverters.asScalaBuffer(projection).toSeq(),
        condition.isPresent() ? new Some<>(condition.get()) : Option.<RexNode>empty(),
        cfg,
        cl);
  }
}
