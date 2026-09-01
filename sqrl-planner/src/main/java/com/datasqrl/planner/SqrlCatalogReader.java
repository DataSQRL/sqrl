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
package com.datasqrl.planner;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.prepare.Prepare;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.catalog.SqlCatalogViewTable;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;

/** Preserves registered SQRL view identities while analyzing relational plans. */
class SqrlCatalogReader extends FlinkCalciteCatalogReader {

  private final TableAnalysisLookup tableLookup;

  SqrlCatalogReader(FlinkCalciteCatalogReader catalogReader, TableAnalysisLookup tableLookup) {
    super(
        catalogReader.getRootSchema(),
        catalogReader.getSchemaPaths(),
        catalogReader.getTypeFactory(),
        catalogReader.getConfig());
    this.tableLookup = tableLookup;
  }

  @Override
  public @Nullable Prepare.PreparingTable getTable(List<String> names) {
    var table = super.getTable(names);
    if (!(table instanceof SqlCatalogViewTable view)) {
      return table;
    }

    var identifier = view.getQualifiedName();
    if (identifier.size() != 3) {
      return table;
    }

    var objectIdentifier =
        ObjectIdentifier.of(identifier.get(0), identifier.get(1), identifier.get(2));
    var viewAnalysis = tableLookup.lookupView(objectIdentifier);
    if (viewAnalysis == null) {
      return table;
    }

    return new SqrlCatalogViewTable(
        view.getRelOptSchema(), viewAnalysis.getRowType(), identifier, view.getStatistic());
  }

  private static class SqrlCatalogViewTable extends FlinkPreparingTableBase {

    SqrlCatalogViewTable(
        org.apache.calcite.plan.RelOptSchema relOptSchema,
        org.apache.calcite.rel.type.RelDataType rowType,
        List<String> names,
        org.apache.flink.table.planner.plan.stats.FlinkStatistic statistic) {
      super(relOptSchema, rowType, names, statistic);
    }
  }
}
