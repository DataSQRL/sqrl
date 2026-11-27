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
package com.datasqrl.planner.contract;

import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator.AddTableResult;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.tables.SourceSinkTableAnalysis;
import com.datasqrl.planner.tables.StorageType;
import com.datasqrl.planner.tags.TableTags;
import com.datasqrl.planner.tags.TagDefinitions;
import com.datasqrl.planner.util.Documented;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTableLike;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;

/**
 * A table that a data contract describes, with what the contract is generated from and compared
 * with. Only the name and the columns are required.
 *
 * @param name the name of the table in its statement
 * @param columns all columns of the table, of which only the physical ones are part of a contract
 * @param watermarkColumn the top-level column that the watermark of the table is declared on
 * @param addsWatermarkColumn whether the statement of the table adds the watermark column itself to
 *     the columns that it takes over from another table with {@code LIKE}
 * @param primaryKey the declared primary key columns, in order
 * @param partitionKeys the declared partition columns, in order
 * @param tags the tags of the table
 * @param storageType the kind of object that stores the table, e.g. a topic
 * @param storageName the name of the object that stores the table
 */
@Builder
public record ContractSubject(
    String name,
    List<Column> columns,
    Optional<String> watermarkColumn,
    boolean addsWatermarkColumn,
    List<String> primaryKey,
    List<String> partitionKeys,
    Documentation documentation,
    TableTags tags,
    Optional<StorageType> storageType,
    Optional<String> storageName) {

  public enum ColumnKind {
    PHYSICAL,
    METADATA,
    COMPUTED
  }

  public record Column(String name, RelDataType type, ColumnKind kind) {}

  public ContractSubject {
    Objects.requireNonNull(name);
    Objects.requireNonNull(columns);
    watermarkColumn = Objects.requireNonNullElse(watermarkColumn, Optional.empty());
    primaryKey = Objects.requireNonNullElse(primaryKey, List.of());
    partitionKeys = Objects.requireNonNullElse(partitionKeys, List.of());
    documentation = Objects.requireNonNullElse(documentation, Documented.EMPTY);
    tags = Objects.requireNonNullElse(tags, TableTags.EMPTY);
    storageType = Objects.requireNonNullElse(storageType, Optional.empty());
    storageName = Objects.requireNonNullElse(storageName, Optional.empty());
  }

  /**
   * @param created a table that a {@code CREATE TABLE} statement defines
   * @param environment has the catalog and the type factory of the table
   */
  public static ContractSubject ofCreatedTable(
      AddTableResult created, Sqrl2FlinkSQLTranslator environment) {

    var table = created.tableAnalysis();
    var sourceSink = table.getSourceSinkTable().orElseThrow();
    var schema = sourceSink.schema();
    var typeFactory = environment.getTypeFactory();

    // The columns are read from the schema and not through the row type of the table, which
    // requires that computed columns come last
    var columns =
        schema.getColumns().stream()
            .map(
                column ->
                    new Column(
                        column.getName(),
                        typeFactory.createFieldTypeFromLogicalType(
                            column.getDataType().getLogicalType()),
                        kindOf(column)))
            .toList();

    var watermarkColumn =
        schema.getWatermarkSpecs().stream()
            .map(WatermarkSpec::getRowtimeAttribute)
            .filter(column -> !column.contains("."))
            .findFirst();

    // The columns that the statement adds to those that it takes over with LIKE
    List<String> addedColumns;
    if (created.createdTable() instanceof SqlCreateTableLike likeTable) {
      addedColumns =
          likeTable.getColumnList().stream()
              .map(column -> ((SqlTableColumn) column).getName().getSimple())
              .toList();
    } else {
      addedColumns = List.of();
    }

    return ContractSubject.builder()
        .name(created.statementName())
        .columns(columns)
        .watermarkColumn(watermarkColumn)
        .addsWatermarkColumn(
            watermarkColumn
                .filter(column -> addedColumns.stream().anyMatch(column::equalsIgnoreCase))
                .isPresent())
        .primaryKey(schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(List.of()))
        .partitionKeys(partitionKeys(created, environment))
        .documentation(created.documentation())
        .tags(table.getTags())
        .storageType(sourceSink.connectorConfig().getStorageType())
        .storageName(sourceSink.connectorConfig().getStorageName())
        .build();
  }

  /**
   * The partition columns as the statement declares them. An engine may remove them from the table
   * and keep them in the mutation, e.g. Kafka uses them as the message key.
   */
  private static List<String> partitionKeys(
      AddTableResult created, Sqrl2FlinkSQLTranslator environment) {
    var mutation =
        created
            .tableAnalysis()
            .getSourceSinkTable()
            .map(SourceSinkTableAnalysis::mutationDefinition);

    if (mutation.isPresent() && !mutation.get().getPartitionKey().isEmpty()) {
      return mutation.get().getPartitionKey();
    }

    return environment
        .getCatalogManager()
        .getTable(created.baseTableIdentifier())
        .map(resolved -> resolved.<ResolvedCatalogTable>getResolvedTable().getPartitionKeys())
        .orElse(List.of());
  }

  /**
   * @param name the name of the table for the systems that read it
   * @param rowType the columns as they are stored, which are all physical
   * @param table the table that the pipeline computes
   * @return a builder with the columns, the documentation and the tags of the table
   */
  public static ContractSubjectBuilder ofComputedTable(
      String name, RelDataType rowType, TableAnalysis table) {
    return ContractSubject.builder()
        .name(name)
        .columns(
            rowType.getFieldList().stream()
                .map(field -> new Column(field.getName(), field.getType(), ColumnKind.PHYSICAL))
                .toList())
        .documentation(table.getDocumentation())
        .tags(table.getTags());
  }

  private static ColumnKind kindOf(org.apache.flink.table.catalog.Column column) {
    if (column instanceof ComputedColumn) {
      return ColumnKind.COMPUTED;
    }
    return column instanceof MetadataColumn ? ColumnKind.METADATA : ColumnKind.PHYSICAL;
  }

  /** The columns that are part of a contract. */
  public List<Column> physicalColumns() {
    return columns.stream().filter(column -> column.kind() == ColumnKind.PHYSICAL).toList();
  }

  public Optional<Column> getColumn(String name) {
    return columns.stream().filter(column -> column.name().equalsIgnoreCase(name)).findFirst();
  }

  /** The tags of a column are the tags in its own documentation. */
  public TableTags columnTags(String column, TagDefinitions definitions) {
    return definitions.parse(documentation.getColumnOpt(column).orElse(null), invalidTag -> {});
  }

  public boolean isWatermarkColumn(String name) {
    return watermarkColumn.filter(column -> column.equalsIgnoreCase(name)).isPresent();
  }

  /**
   * A contract may leave out the watermark column if the table adds it itself to the columns of the
   * table it extends: those columns are the data, and how an environment provides the event time is
   * up to its connector table. A watermark column that the table takes over is data.
   */
  public boolean mayOmitColumn(String name) {
    return addsWatermarkColumn && isWatermarkColumn(name);
  }
}
