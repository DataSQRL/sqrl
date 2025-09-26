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

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.NonNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

final class CatalogUtils {

  private static final String DEFAULT_DB = "default_database";

  static <T> T executeInCatalog(
      @NonNull String warehouse,
      @Nullable String catalogType,
      @NonNull String catalogName,
      @Nullable String database,
      @NonNull String tableName,
      @NonNull Function<Table, T> fn) {

    database = database == null ? DEFAULT_DB : database;

    if ("hadoop".equalsIgnoreCase(catalogType)) {
      return executeInHadoopCatalog(warehouse, database, tableName, fn);
    }

    // Fallback to Glue for now
    return executeInGlueCatalog(warehouse, catalogName, database, tableName, fn);
  }

  private static <T> T executeInHadoopCatalog(
      String warehouse, String database, String tableName, Function<Table, T> fn) {

    try (var catalog = new HadoopCatalog(new Configuration(), warehouse)) {
      var table = catalog.loadTable(TableIdentifier.of(database, tableName));

      return fn.apply(table);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> T executeInGlueCatalog(
      String warehouse,
      String catalogName,
      String database,
      String tableName,
      Function<Table, T> fn) {

    try (var catalog = new GlueCatalog()) {
      catalog.initialize(catalogName, Map.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse));
      var table = catalog.loadTable(TableIdentifier.of(database, tableName));

      return fn.apply(table);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private CatalogUtils() {
    throw new UnsupportedOperationException();
  }
}
