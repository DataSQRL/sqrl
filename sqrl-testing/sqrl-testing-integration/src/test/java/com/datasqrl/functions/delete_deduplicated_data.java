/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.functions;

import com.google.auto.service.AutoService;
import java.util.function.Function;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.iceberg.Table;

@AutoService(ScalarFunction.class)
public class delete_deduplicated_data extends ScalarFunction {

  public Long eval(
      String warehouse,
      String catalogType,
      String catalogName,
      String db,
      String tableName,
      String partitionCol,
      Long partitionVal) {

    if (warehouse == null
        || catalogType == null
        || tableName == null
        || partitionCol == null
        || partitionVal == null) {
      return null;
    }

    var delFn = getDeleteFunction(partitionCol, partitionVal);

    return CatalogUtils.executeInCatalog(warehouse, catalogType, catalogName, db, tableName, delFn);
  }

  public Long eval(
      String warehouse,
      String catalogType,
      String catalogName,
      String db,
      String tableName,
      String partitionCol,
      Long[] partitionVals) {

    if (warehouse == null
        || catalogType == null
        || tableName == null
        || partitionCol == null
        || partitionVals == null
        || partitionVals.length == 0) {
      return null;
    }

    var delFn = getDeleteFunction(partitionCol, partitionVals);

    return CatalogUtils.executeInCatalog(warehouse, catalogType, catalogName, db, tableName, delFn);
  }

  private Function<Table, Long> getDeleteFunction(String partitionCol, Long partitionVal) {
    return table -> {
      var delExpr = ExpressionUtils.buildExclusiveInterval(partitionCol, 0, partitionVal);
      var delFiles = table.newDelete().deleteFromRowFilter(delExpr);
      delFiles.commit();

      return partitionVal;
    };
  }

  private Function<Table, Long> getDeleteFunction(String partitionCol, Long[] partitionVals) {
    return table -> {
      var delExpr = ExpressionUtils.buildEqualsList(partitionCol, partitionVals);
      var delFiles = table.newDelete().deleteFromRowFilter(delExpr);
      delFiles.commit();

      // TODO: what to return?
      return null;
    };
  }
}
