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

import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.calcite.rel.core.TableScan;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

/** Maintains table analysis keyed by Flink catalog identity. */
@Value
public class TableAnalysisLookup {

  Map<ObjectIdentifier, TableAnalysis> id2SourceTable = new HashMap<>();
  Map<ObjectIdentifier, TableAnalysis> id2View = new HashMap<>();

  public TableAnalysis lookupSourceTable(@Nullable ObjectIdentifier objectId) {
    return id2SourceTable.get(objectId);
  }

  @Nullable
  public TableAnalysis lookupViewFromScan(@Nullable TableScan tableScan) {
    var table = tableScan.getTable();
    ObjectIdentifier identifier = null;
    if (table instanceof TableSourceTable sourceTable) {
      identifier = sourceTable.contextResolvedTable().getIdentifier();
    } else if (table instanceof FlinkPreparingTableBase preparingTable) {
      var names = preparingTable.getQualifiedName();
      if (names.size() == 3) {
        identifier = ObjectIdentifier.of(names.get(0), names.get(1), names.get(2));
      }
    } else if (table instanceof CatalogSourceTable catalogTable) {
      var names = catalogTable.getQualifiedName();
      if (names.size() == 3) {
        identifier = ObjectIdentifier.of(names.get(0), names.get(1), names.get(2));
      }
    }
    return identifier == null ? null : id2View.get(identifier);
  }

  public TableAnalysis lookupView(ObjectIdentifier objectIdentifier) {
    return id2View.get(objectIdentifier);
  }

  public void removeView(ObjectIdentifier tableIdentifier) {
    id2View.remove(tableIdentifier);
  }

  public void registerTable(TableAnalysis tableAnalysis) {
    if (tableAnalysis.isSourceOrSink()) {
      id2SourceTable.put(tableAnalysis.getObjectIdentifier(), tableAnalysis);
    } else {
      id2View.put(tableAnalysis.getObjectIdentifier(), tableAnalysis);
    }
  }
}
