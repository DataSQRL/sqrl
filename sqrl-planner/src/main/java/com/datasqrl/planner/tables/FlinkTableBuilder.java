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
package com.datasqrl.planner.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory;
import com.datasqrl.graphql.server.ResolvedMetadata;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

/**
 * A builder for tables in Flink that is used as the central table builder in DataSQRL. It's a light
 * wrapper around the SqlNode relying heavily on {@link FlinkSqlNodeFactory} for translation.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlinkTableBuilder {

  private SqlIdentifier tableName = null; // required, no default

  private SqlNodeList columnList = null; // required, no default

  private SqlNodeList propertyList = SqlNodeList.EMPTY;

  private List<SqlTableConstraint> tableConstraints = Collections.emptyList();

  private SqlNodeList partitionKeyList = SqlNodeList.EMPTY;

  private SqlWatermark watermark = null;

  public FlinkTableBuilder setName(Name name) {
    setName(name.getDisplay());
    return this;
  }

  public FlinkTableBuilder setName(String name) {
    setTableName(FlinkSqlNodeFactory.identifier(name));
    return this;
  }

  public String getTableName() {
    return tableName.getSimple();
  }

  public FlinkTableBuilder setColumns(RelDataType relDataType) {
    setColumnList(FlinkSqlNodeFactory.createColumns(relDataType));
    return this;
  }

  public FlinkTableBuilder addColumns(RelDataType relDataType) {
    SqlNodeList addCols = FlinkSqlNodeFactory.createColumns(relDataType);
    List<SqlNode> nodes = new ArrayList<>(addCols.getList());
    if (columnList != null) {
      nodes.addAll(columnList.getList());
    }
    setColumnList(new SqlNodeList(nodes, SqlParserPos.ZERO));
    return this;
  }

  public Map<String, ResolvedMetadata> extractMetadataColumns(MetadataExtractor extractor) {
    Preconditions.checkArgument(columnList != null, "Need to initialize columns first");
    Map<String, ResolvedMetadata> convertedColumns = new HashMap<>();
    for (var i = 0; i < columnList.size(); i++) {
      var column = columnList.get(i);
      if (column instanceof SqlTableColumn.SqlMetadataColumn columnMetadata) {
        ResolvedMetadata metadata =
            columnMetadata
                .getMetadataAlias()
                .map(alias -> extractor.convert(alias, columnMetadata.getType().getNullable()))
                .orElse(null);
        if (metadata != null) {
          convertedColumns.put(columnMetadata.getName().getSimple(), metadata);
        }
        // Remove metadata if extractor says so
        if (columnMetadata.getMetadataAlias().map(extractor::removeMetadata).orElse(false)) {
          var regularColumn =
              new SqlRegularColumn(
                  columnMetadata.getParserPosition(),
                  columnMetadata.getName(),
                  columnMetadata.getComment().orElse(null),
                  columnMetadata.getType(),
                  null);
          columnList.set(i, regularColumn);
        }
      }
    }
    return convertedColumns;
  }

  public FlinkTableBuilder setConnectorOptions(Map<String, String> options) {
    setPropertyList(FlinkSqlNodeFactory.createProperties(options));
    return this;
  }

  public Map<String, String> getConnectorOptions() {
    return FlinkSqlNodeFactory.propertiesToMap(getPropertyList());
  }

  public FlinkTableBuilder setDummyConnector() {
    return setConnectorOptions(Map.of(FlinkConnectorConfig.CONNECTOR_KEY, "datagen"));
  }

  public FlinkTableBuilder setWatermarkMillis(String timestampColumnName, long watermarkMillis) {
    setWatermark(FlinkSqlNodeFactory.createWatermark(timestampColumnName, watermarkMillis));
    return this;
  }

  public FlinkTableBuilder setPartition(List<String> partitionColumns) {
    if (partitionColumns.isEmpty()) {
      setPartitionKeyList(SqlNodeList.EMPTY);
    }
    setPartitionKeyList(FlinkSqlNodeFactory.createPartitionKeys(partitionColumns));
    return this;
  }

  public boolean hasPartition() {
    return !getPartitionKeyList().isEmpty();
  }

  public List<String> getPartition() {
    return StreamUtil.filterByClass(getPartitionKeyList().getList(), SqlIdentifier.class)
        .map(SqlIdentifier::getSimple)
        .collect(Collectors.toList());
  }

  public FlinkTableBuilder setPrimaryKey(List<String> pkColumns) {
    if (pkColumns.isEmpty()) {
      setTableConstraints(List.of());
    }
    var pkConstraint = FlinkSqlNodeFactory.createPrimaryKeyConstraint(pkColumns);
    setTableConstraints(Collections.singletonList(pkConstraint));
    return this;
  }

  public FlinkTableBuilder removePrimaryKey() {
    setTableConstraints(Collections.emptyList());
    return this;
  }

  public Optional<List<String>> getPrimaryKey() {
    if (tableConstraints.isEmpty()) {
      return Optional.empty();
    }
    Preconditions.checkArgument(tableConstraints.size() == 1, "Expected a single table constraint");
    var pkConstraint = tableConstraints.get(0);
    Preconditions.checkArgument(pkConstraint.isPrimaryKey(), "Expected a primary key constraint");
    return Optional.of(Arrays.asList(pkConstraint.getColumnNames()));
  }

  public boolean hasPrimaryKey() {
    return getPrimaryKey().isPresent();
  }

  public SqlCreateTable buildSql(boolean isTemporary) {
    return new SqlCreateTable(
        SqlParserPos.ZERO,
        tableName,
        columnList,
        tableConstraints,
        propertyList,
        //        FlinkSqlNodeFactory.NO_DISTRIBUTION,
        partitionKeyList,
        watermark,
        null,
        isTemporary,
        false);
  }

  public static FlinkTableBuilder toBuilder(SqlCreateTable table) {
    return new FlinkTableBuilder(
        table.getTableName(),
        table.getColumnList(),
        table.getPropertyList(),
        table.getTableConstraints(),
        table.getPartitionKeyList(),
        table.getWatermark().orElse(null));
  }
}
