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
package com.datasqrl.engine.stream.flink;

import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;

/** Encapsulates access to Flink's SQL planning facilities. */
@RequiredArgsConstructor
public final class FlinkSqlNodePlanner {

  private final TableEnvironmentImpl tableEnvironment;
  private final Supplier<FlinkPlannerImpl> plannerSupplier;

  public RelDataType getValidatedNodeType(SqlNode statement) {
    var sql = RelToFlinkSql.convertToString(statement);
    var planner = plannerSupplier.get();
    var validated = planner.validate(FlinkCalciteParser.parseSql(sql, tableEnvironment));
    return planner.getOrCreateSqlValidator().getValidatedNodeType(validated);
  }

  public RelRoot toRelRoot(SqlNode query, @Nullable FlinkPlannerImpl flinkPlanner) {
    var planner = getPlanner(flinkPlanner);
    var validatedQuery = planner.getOrCreateSqlValidator().validate(query);
    return planner.rel(validatedQuery);
  }

  public FlinkRelBuilder getRelBuilder(@Nullable FlinkPlannerImpl flinkPlanner) {
    var planner = getPlanner(flinkPlanner);
    var config =
        planner.config().getSqlToRelConverterConfig().withAddJsonTypeOperatorEnabled(false);
    // A null schema prevents the builder's scan method from expanding views.
    return (FlinkRelBuilder)
        config
            .getRelBuilderFactory()
            .create(planner.cluster(), null)
            .transform(config.getRelBuilderConfigTransform());
  }

  public CalciteCatalogReader getCalciteCatalog(@Nullable FlinkPlannerImpl flinkPlanner) {
    return getPlanner(flinkPlanner)
        .getOrCreateSqlValidator()
        .getCatalogReader()
        .unwrap(CalciteCatalogReader.class);
  }

  private FlinkPlannerImpl getPlanner(@Nullable FlinkPlannerImpl planner) {
    return planner == null ? plannerSupplier.get() : planner;
  }
}
