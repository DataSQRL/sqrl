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
package com.datasqrl.util;

import static com.datasqrl.config.SqrlConstants.FLINK_DEFAULT_CATALOG;
import static com.datasqrl.config.SqrlConstants.FLINK_DEFAULT_DATABASE;
import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_DATABASE_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_IMPL_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_TABLE_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_TYPE_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_WAREHOUSE_KEY;
import static com.datasqrl.planner.tables.FlinkConnectorConfigWrapper.CONNECTOR_KEY;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.ddl.catalog.SqlUseCatalog;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.table.SqlTableLike;
import org.apache.flink.sql.parser.ddl.table.SqlTableLike.FeatureOption;
import org.apache.flink.sql.parser.ddl.table.SqlTableLike.MergingStrategy;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.sql.parser.dml.SqlStatementSet;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.parse.CalciteParser;

/**
 * Finds the Iceberg tables that a compiled Flink plan writes into through the {@code iceberg}
 * connector, from the {@code CREATE TABLE} and {@code INSERT INTO} statements of its Flink SQL.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class IcebergSinkTables {

  /** The parser configuration of the Flink planner */
  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config()
          .withParserFactory(FlinkSqlParserImpl.FACTORY)
          .withConformance(FlinkSqlConformance.DEFAULT)
          .withLex(Lex.JAVA)
          .withIdentifierMaxLength(256);

  private static final String ICEBERG_CONNECTOR = "iceberg";
  private static final String HADOOP_CATALOG_TYPE = "hadoop";
  private static final String FILE_SCHEME = "file";

  /** A URI scheme of at least two characters, so that a Windows drive letter is not one */
  private static final Pattern URI_SCHEME = Pattern.compile("^([a-zA-Z][a-zA-Z0-9+.-]+):");

  /**
   * An Iceberg table that the Flink plan writes into.
   *
   * @param name the name of the Flink table
   * @param options the connector options of the Flink table, with the environment variables
   *     unresolved, as they are in the plan
   */
  record IcebergSinkTable(String name, Map<String, String> options) {

    /**
     * Returns the directory of the table if it is in a Hadoop catalog with a warehouse on the local
     * file system. A relative warehouse is resolved against the working directory, as Flink does.
     *
     * @param resolveEnv resolves the environment variables in an option value
     * @return the table directory, or empty for any other catalog or warehouse
     */
    Optional<Path> localDirectory(UnaryOperator<String> resolveEnv) {
      if (options.containsKey(ICEBERG_CATALOG_IMPL_KEY)
          || !HADOOP_CATALOG_TYPE.equalsIgnoreCase(option(ICEBERG_CATALOG_TYPE_KEY, resolveEnv))) {
        return Optional.empty();
      }

      return localPath(option(ICEBERG_WAREHOUSE_KEY, resolveEnv))
          .flatMap(
              warehouse -> {
                var database =
                    resolveEnv.apply(
                        options.getOrDefault(ICEBERG_CATALOG_DATABASE_KEY, FLINK_DEFAULT_DATABASE));
                var table = resolveEnv.apply(options.getOrDefault(ICEBERG_CATALOG_TABLE_KEY, name));
                var tableDir = warehouse.resolve(database).resolve(table).normalize();

                // A Hadoop catalog keeps a table two levels below the warehouse; anything else
                // would come from a database or table name that is a path
                return tableDir.startsWith(warehouse)
                        && warehouse.relativize(tableDir).getNameCount() == 2
                    ? Optional.of(tableDir)
                    : Optional.empty();
              });
    }

    private String option(String key, UnaryOperator<String> resolveEnv) {
      var value = options.get(key);
      return value == null ? null : resolveEnv.apply(value);
    }

    private static Optional<Path> localPath(String warehouse) {
      if (warehouse == null || warehouse.isBlank() || warehouse.contains("${")) {
        return Optional.empty();
      }

      var scheme = URI_SCHEME.matcher(warehouse);
      if (!scheme.find()) {
        return Optional.of(Path.of(warehouse).toAbsolutePath().normalize());
      }
      if (!FILE_SCHEME.equalsIgnoreCase(scheme.group(1))) {
        return Optional.empty();
      }

      try {
        return Optional.of(Path.of(URI.create(warehouse)).normalize());
      } catch (IllegalArgumentException e) {
        log.debug("Iceberg warehouse '{}' is not a local path", warehouse, e);
        return Optional.empty();
      }
    }
  }

  /**
   * Finds the Iceberg tables that the given Flink SQL statements write into.
   *
   * @param flinkSql the Flink SQL statements of the plan, in order
   * @return the Iceberg tables in the order the plan inserts into them
   */
  static List<IcebergSinkTable> find(List<String> flinkSql) {
    var collector = new Collector();
    flinkSql.forEach(collector::add);
    return collector.icebergSinks();
  }

  /** Collects the table definitions and insert targets, keeping track of USE statements. */
  private static class Collector {

    private final CalciteParser parser = new CalciteParser(PARSER_CONFIG);
    private final Map<List<String>, Map<String, String>> tableOptions = new HashMap<>();
    private final Set<List<String>> insertTargets = new LinkedHashSet<>();
    private String catalog = FLINK_DEFAULT_CATALOG;
    private String database = FLINK_DEFAULT_DATABASE;

    void add(String statement) {
      SqlNodeList nodes;
      try {
        nodes = parser.parseSqlList(statement);
      } catch (RuntimeException e) {
        log.debug("Skipping Flink SQL statement that cannot be parsed: {}", statement, e);
        return;
      }
      nodes.forEach(this::add);
    }

    private void add(SqlNode node) {
      if (node instanceof SqlUseCatalog useCatalog) {
        catalog = useCatalog.catalogName();
      } else if (node instanceof SqlUseDatabase useDatabase) {
        var names = useDatabase.getDatabaseName().names;
        if (names.size() > 1) {
          catalog = names.get(0);
        }
        database = names.get(names.size() - 1);
      } else if (node instanceof SqlCreateTable createTable) {
        addTable(createTable);
      } else if (node instanceof RichSqlInsert insert) {
        if (insert.getTargetTableID() instanceof SqlIdentifier target) {
          insertTargets.add(qualify(target));
        }
      } else if (node instanceof SqlStatementSet statementSet) {
        statementSet.getInserts().forEach(this::add);
      } else if (node instanceof SqlExecute execute) {
        add(execute.getStatement());
      }
    }

    private void addTable(SqlCreateTable createTable) {
      var options = new HashMap<String, String>();
      if (createTable instanceof SqlCreateTableLike createTableLike
          && inheritsOptions(createTableLike.getTableLike())) {
        var source = qualify(createTableLike.getTableLike().getSourceTable());
        options.putAll(tableOptions.getOrDefault(source, Map.of()));
      }
      options.putAll(createTable.getProperties());
      tableOptions.put(qualify(createTable.getName()), options);
    }

    /** Whether a LIKE clause copies the options of its source table, which Flink does by default */
    private static boolean inheritsOptions(SqlTableLike tableLike) {
      var inherits = true;
      for (var likeOption : tableLike.getOptions()) {
        var feature = likeOption.getFeatureOption();
        if (feature == FeatureOption.ALL || feature == FeatureOption.OPTIONS) {
          inherits = likeOption.getMergingStrategy() != MergingStrategy.EXCLUDING;
        }
      }
      return inherits;
    }

    private List<String> qualify(SqlIdentifier identifier) {
      var names = identifier.names;
      return switch (names.size()) {
        case 1 -> List.of(catalog, database, names.get(0));
        case 2 -> List.of(catalog, names.get(0), names.get(1));
        default -> List.copyOf(names);
      };
    }

    List<IcebergSinkTable> icebergSinks() {
      return insertTargets.stream()
          .filter(tableOptions::containsKey)
          .map(id -> new IcebergSinkTable(id.get(id.size() - 1), Map.copyOf(tableOptions.get(id))))
          .filter(table -> ICEBERG_CONNECTOR.equalsIgnoreCase(table.options().get(CONNECTOR_KEY)))
          .toList();
    }
  }
}
