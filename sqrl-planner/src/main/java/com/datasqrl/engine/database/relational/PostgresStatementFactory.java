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
package com.datasqrl.engine.database.relational;

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.database.relational.ddl.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.InsertStatement;
import com.datasqrl.engine.database.relational.ddl.PostgresCreateTableDdlFactory;
import com.datasqrl.engine.database.relational.ddl.notify.CreateNotifyTriggerDDL;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.planner.hint.DataTypeHint;
import com.datasqrl.planner.hint.VectorDimensionHint;
import com.datasqrl.sql.DatabaseTableExtension;
import com.datasqrl.sql.DatabaseTypeExtension;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.parser.SqlParserPos;

public class PostgresStatementFactory extends AbstractJdbcStatementFactory {

  public static final String PARTITION_DIVISOR_KEY = "partition-divisor";
  public static final int DEFAULT_PARTITION_DIVISOR = 100;

  /**
   * Calendar-aligned partition widths pg_partman can use, in minutes, with their interval labels.
   */
  private static final long[] PARTITION_MENU_MINUTES = {
    15, 30, 60, 120, 240, 360, 480, 720, 1440, 2880, 5760, 10080, 20160, 40320, 80640, 120960
  };

  private static final String[] PARTITION_MENU_LABELS = {
    "15 minutes", "30 minutes", "1 hour", "2 hours", "4 hours", "6 hours", "8 hours", "12 hours",
    "1 day", "2 days", "4 days", "1 week", "2 weeks", "4 weeks", "8 weeks", "12 weeks"
  };

  private final int partitionDivisor;

  public PostgresStatementFactory() {
    this(DEFAULT_PARTITION_DIVISOR);
  }

  public PostgresStatementFactory(EngineConfig engineConfig) {
    this(parsePartitionDivisor(engineConfig));
  }

  public PostgresStatementFactory(int partitionDivisor) {
    super(Dialect.POSTGRES, new PostgresCreateTableDdlFactory(true));
    checkArgument(partitionDivisor > 0, "%s must be a positive number", PARTITION_DIVISOR_KEY);
    this.partitionDivisor = partitionDivisor;
  }

  private static int parsePartitionDivisor(EngineConfig engineConfig) {
    var setting =
        engineConfig.getSetting(
            PARTITION_DIVISOR_KEY, Optional.of(String.valueOf(DEFAULT_PARTITION_DIVISOR)));
    try {
      return Integer.parseInt(setting.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "%s must be a positive number, but was: %s".formatted(PARTITION_DIVISOR_KEY, setting), e);
    }
  }

  @Override
  public JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  protected SqlDataTypeSpec getSqlType(RelDataType type, Optional<DataTypeHint> hint) {
    SqlDataTypeSpec spec = ExtendedPostgresSqlDialect.DEFAULT.getCastSpec(type);
    Optional<VectorDimensionHint> vecDimOpt =
        hint.filter(VectorDimensionHint.class::isInstance).map(VectorDimensionHint.class::cast);
    if (vecDimOpt.isPresent()) {
      spec =
          new SqlDataTypeSpec(
              new SqlAlienSystemTypeNameSpec(
                  "VECTOR(" + vecDimOpt.get().getDimensions() + ")",
                  type.getSqlTypeName(),
                  SqlParserPos.ZERO),
              SqlParserPos.ZERO);
    }
    return spec;
  }

  @Override
  protected PartitionType getPartitionType(
      JdbcEngineCreateTable createTable, List<String> partitionKey) {
    if (partitionKey.isEmpty()) {
      return PartitionType.NONE;
    }

    checkArgument(
        partitionKey.size() == 1,
        "Postgres only supports partitions for a single column on table %s. Combine these columns into one: %s",
        createTable.tableName(),
        partitionKey);

    String partitionCol = partitionKey.get(0);
    var colType = createTable.tableAnalysis().getRowType().getField(partitionCol, false, false);

    // Look up field reldatatype to determine partition type
    return CalciteUtil.isTimestamp(colType.getType()) ? PartitionType.RANGE : PartitionType.HASH;
  }

  @Override
  protected String derivePartitionInterval(
      PartitionType partitionType, Duration ttl, ChronoUnit ttlUnit) {
    if (partitionType != PartitionType.RANGE || ttl == null || ttl.isZero() || ttlUnit == null) {
      return null;
    }
    return derivePartitionInterval(ttl, ttlUnit, partitionDivisor);
  }

  /**
   * Picks the partition width for a range-partitioned table with a TTL: the TTL divided by the
   * configured divisor caps the partition count, while the unit the TTL was declared with sets the
   * floor. The result is snapped down to the closest calendar-aligned width from the menu.
   */
  static String derivePartitionInterval(Duration ttl, ChronoUnit ttlUnit, int partitionDivisor) {
    var floorMinutes = ttlUnit.getDuration().toMinutes();
    var targetMinutes = Math.max(ttl.toMinutes() / (double) partitionDivisor, floorMinutes);
    var interval = PARTITION_MENU_LABELS[0];
    for (var i = 0; i < PARTITION_MENU_MINUTES.length; i++) {
      if (PARTITION_MENU_MINUTES[i] <= targetMinutes) {
        interval = PARTITION_MENU_LABELS[i];
      }
    }
    return interval;
  }

  @Override
  public List<JdbcStatement> applyTableExtensions(Collection<CreateTableJdbcStatement> tables) {
    var res = new ArrayList<JdbcStatement>();

    var tableExtensions = ServiceLoaderDiscovery.getAll(DatabaseTableExtension.class);
    for (var ext : tableExtensions) {
      var ddl = ext.getDdl(tables);

      if (ddl != null && !ddl.isBlank()) {
        res.add(new GenericJdbcStatement(ext.getName(), Type.EXTENSION, ddl));
      }
    }

    return List.copyOf(res);
  }

  @Override
  public List<JdbcStatement> extractTypeExtensions(List<Query> queries) {
    var typeExtensions = ServiceLoaderDiscovery.getAll(DatabaseTypeExtension.class);

    return extractTypeExtensions(queries.stream().map(Query::relNode), typeExtensions).stream()
        .map(
            ext ->
                new GenericJdbcStatement(
                    ext.getClass().getSimpleName(), Type.EXTENSION, ext.getDdl()))
        .collect(Collectors.toList());
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition index) {
    var ddl =
        new CreateIndexDDL(
            index.getName(), index.getTableName(), index.getColumnNames(), index.getType());
    return new GenericJdbcStatement(ddl.getIndexName(), Type.INDEX, ddl.getSql());
  }

  /*
  The following methods are for the Postgres Log engine - we'll keep those around for now
  */

  public CreateNotifyTriggerDDL createNotify(String name, List<String> primaryKeys) {
    return new CreateNotifyTriggerDDL(name, primaryKeys);
  }

  public InsertStatement createInsertHelperDMLs(String tableName, RelDataType tableSchema) {
    return new InsertStatement(tableName, tableSchema);
  }
}
